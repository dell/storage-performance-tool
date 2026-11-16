package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.dell.spt.base.item.op.Operation.SLASH;
import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.dell.spt.storage.driver.coop.netty.http.s3.S3Api.TAGGING_ENTRY_END;
import static com.dell.spt.storage.driver.coop.netty.http.s3.S3Api.TAGGING_ENTRY_MIDDLE;
import static com.dell.spt.storage.driver.coop.netty.http.s3.S3Api.TAGGING_ENTRY_START;
import static com.dell.spt.storage.driver.coop.netty.http.s3.S3Api.TAGGING_FOOTER;
import static com.dell.spt.storage.driver.coop.netty.http.s3.S3Api.TAGGING_HEADER;
import static com.github.akurilov.commons.io.el.ExpressionInput.ASYNC_MARKER;
import static com.github.akurilov.commons.io.el.ExpressionInput.INIT_MARKER;
import static com.github.akurilov.commons.io.el.ExpressionInput.SYNC_MARKER;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.config.el.CompositeExpressionInputBuilder;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.DateUtil;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.InvalidTextValueException;
import com.dell.spt.base.integrity.IntegrityCsvArtifacts;
import com.dell.spt.base.integrity.IntegrityMetadataCodec;
import com.dell.spt.base.integrity.IntegrityVerificationResult;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemNamePaths;
import com.dell.spt.base.item.io.TerminalItemInputException;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteTarget;
import com.dell.spt.base.item.op.deletion.DeleteTransportResult;
import com.dell.spt.base.item.op.deletion.DeleteVerificationProbe;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.item.op.list.ListOperation;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker.DispatchToken;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.bucket.AwsS3Region;
import com.dell.spt.base.storage.driver.bucket.BucketVersioningNotice;
import com.dell.spt.base.storage.driver.bucket.TargetBucketResponse;
import com.dell.spt.base.storage.driver.ListOptions;
import com.dell.spt.storage.driver.coop.netty.http.HttpStorageDriverBase;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.confuse.Config;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpStatusClass;
import io.netty.handler.codec.http.HttpVersion;
import software.amazon.awssdk.crt.checksums.CRC64NVME;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.Level;
import org.xml.sax.SAXException;

/** Created by kurila on 01.08.16. */
public class S3StorageDriver<I extends Item, O extends Operation<I>>
				extends HttpStorageDriverBase<I, O>
				implements com.dell.spt.base.storage.driver.ListDiscoveryProbe, DeleteVerificationProbe {

	private static final String REDACTED_HEADER_VALUE = "<redacted>";
	/**
	 * How long an abort that follows a failed multipart completion may keep the stop waiting, counted
	 * from when the abort was scheduled, or from the stop's start for one scheduled during the stop. An
	 * abort carries no data and a target answers it in milliseconds, so 5 s leaves room for a loaded
	 * target. The bound is half the entry node's RMI response timeout
	 * ({@code -Dsun.rmi.transport.tcp.responseTimeout=10000} in the image and {@code run.sh}): a
	 * worker's step slice stops inside that RMI call and stops its contexts one after another, and
	 * counting from the schedule keeps the aborts pending at the step's stop within 5 s in all,
	 * whatever the number of contexts. It does not depend on {@code storage.net.timeoutMilliSec}, so it
	 * holds at 0 too; an abort still unanswered then ends when the I/O shutdown closes its connection.
	 */
	static final long MULTIPART_CLEANUP_STOP_WAIT_MILLIS = 5_000;
	/**
	 * The close's own bound on the same aborts when {@code storage.net.timeoutMilliSec} is 0 or less,
	 * where the request has no timeout of its own: 60 s, twice the default timeout, as below.
	 */
	static final long MULTIPART_CLEANUP_CLOSE_CAP_MILLIS = 60_000;
	/** Above twice the net timeout, for building and signing the abort and scheduling its thread. */
	static final long MULTIPART_CLEANUP_CLOSE_MARGIN_MILLIS = 1_000;
	static final String DELETE_FAILURE_MESSAGE = "S3 DELETE request failed at the transport or service boundary";
	private static final String DELETE_QUERY = "delete";
	private static final String DELETE_REQUEST_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
					+ "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";
	private static final String DELETE_REQUEST_OBJECT_START = "<Object><Key>";
	private static final String DELETE_REQUEST_KEY_END = "</Key>";
	private static final String DELETE_REQUEST_VERSION_START = "<VersionId>";
	private static final String DELETE_REQUEST_VERSION_END = "</VersionId>";
	private static final String DELETE_REQUEST_OBJECT_END = "</Object>";
	private static final String DELETE_REQUEST_FOOTER = "<Quiet>false</Quiet></Delete>";

	protected static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
	protected static final ThreadLocal<SAXParser> THREAD_LOCAL_XML_PARSER = new ThreadLocal<>();
	protected static final ThreadLocal<StringBuilder> BUFF_CANONICAL = ThreadLocal.withInitial(StringBuilder::new),
					BUCKET_LIST_QUERY = ThreadLocal.withInitial(StringBuilder::new);
	//for v4 only
	// TODO: compare to ConcurrentHashMap
	// Each thread's signing function per secret. SPT's own function keeps that secret's key of the last
	// date itself; a function an extension puts here is memoized by date in signingKeyCache.
	protected final ThreadLocal<Map<String, SigningKeyCreateFunction>> signingKeyCreateFunctionCache = ThreadLocal.withInitial(HashMap::new);
	// Each thread's keys by date, for functions an extension puts in signingKeyCreateFunctionCache only.
	// It keeps one key per date, whatever the secret, as it always has; SPT's own keys are not in it,
	// and SPT's own function never reads it, so a key put here is used only by such a function.
	protected final ThreadLocal<Map<String, byte[]>> signingKeyCache = ThreadLocal.withInitial(HashMap::new);
	// for v2 only
	protected static final ThreadLocal<Map<String, Mac>> MAC_BY_SECRET = ThreadLocal.withInitial(HashMap::new);
	protected static final Function<String, Mac> GET_MAC_BY_SECRET = secret -> {
		final var secretKey = new SecretKeySpec(secret.getBytes(UTF_8), S3Api.SIGN_METHOD);
		try {
			final var mac = Mac.getInstance(S3Api.SIGN_METHOD);
			mac.init(secretKey);
			return mac;
		} catch (final NoSuchAlgorithmException | InvalidKeyException e) {
			throw new AssertionError(e);
		}
	};
	private static final ThreadLocal<MessageDigest> THREAD_LOCAL_MD5 = ThreadLocal.withInitial(
					() -> {
						try {
							return MessageDigest.getInstance("MD5");
						} catch (final NoSuchAlgorithmException e) {
							throw new AssertionError(e);
						}
					});
	// sha256 digest and md5 digest are used for different purposes
	private static final ThreadLocal<MessageDigest> THREAD_LOCAL_SHA256 = ThreadLocal.withInitial(
					() -> {
						try {
							return MessageDigest.getInstance("SHA-256");
						} catch (final NoSuchAlgorithmException e) {
							throw new AssertionError(e);
						}
					});

	// Additional S3 checksums
	// https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/s3-checksums.html
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
	static class ChecksumMessageDigest extends MessageDigest {
		private final Checksum checksum;
		private final int digestWidthBytes;

		public ChecksumMessageDigest(Checksum checksum, String algorithm) {
			this(checksum, algorithm, Integer.BYTES);
		}

		public ChecksumMessageDigest(Checksum checksum, String algorithm, int digestWidthBytes) {
			super(algorithm);
			this.checksum = checksum;
			this.digestWidthBytes = digestWidthBytes;
		}

		@Override
		protected void engineUpdate(byte input) {
			checksum.update(input);
		}

		@Override
		protected void engineUpdate(byte[] input, int offset, int len) {
			checksum.update(input, offset, len);
		}

		@Override
		protected byte[] engineDigest() {
			ByteBuffer buffer = ByteBuffer.allocate(digestWidthBytes);
			if (digestWidthBytes == Integer.BYTES) {
				buffer.putInt((int) (checksum.getValue() & 0xFFFFFFFFL)); // Amazon is expecting this
			} else if (digestWidthBytes == Long.BYTES) {
				buffer.putLong(checksum.getValue());
			} else {
				throw new AssertionError("Unsupported checksum width: " + digestWidthBytes);
			}
			return buffer.array();
		}

		@Override
		protected void engineReset() {
			checksum.reset();
		}
	}

	private static final ThreadLocal<MessageDigest> THREAD_LOCAL_CRC32 = ThreadLocal.withInitial(
					() -> {
						return new ChecksumMessageDigest(new CRC32(), "CRC32");
					});
	private static final ThreadLocal<MessageDigest> THREAD_LOCAL_CRC32C = ThreadLocal.withInitial(
					() -> {
						return new ChecksumMessageDigest(new CRC32C(), "CRC32C");
					});
	private static final ThreadLocal<MessageDigest> THREAD_LOCAL_CRC64_NVME = ThreadLocal.withInitial(
					() -> {
						return new ChecksumMessageDigest(new CRC64NVME(), "CRC64NVME", Long.BYTES);
					});
	private static final ThreadLocal<MessageDigest> THREAD_LOCAL_SHA1 = ThreadLocal.withInitial(
					() -> {
						try {
							return MessageDigest.getInstance("SHA-1");
						} catch (NoSuchAlgorithmException e) {
							throw new AssertionError(e);
						}
					});

	static final class ChecksumStrategy {
		final String configToken;
		final String requestHeaderName;
		final String mpuInitAlgorithmToken;
		final String completeMultipartXmlElementName;
		final String responseChecksumHeaderName;
		final Supplier<MessageDigest> digestSupplier;

		ChecksumStrategy(
						final String configToken,
						final String requestHeaderName,
						final String mpuInitAlgorithmToken,
						final String completeMultipartXmlElementName,
						final String responseChecksumHeaderName,
						final Supplier<MessageDigest> digestSupplier) {
			this.configToken = configToken;
			this.requestHeaderName = requestHeaderName;
			this.mpuInitAlgorithmToken = mpuInitAlgorithmToken;
			this.completeMultipartXmlElementName = completeMultipartXmlElementName;
			this.responseChecksumHeaderName = responseChecksumHeaderName;
			this.digestSupplier = digestSupplier;
		}
	}

	private static final Map<String, ChecksumStrategy> CHECKSUM_STRATEGIES = createChecksumStrategies();

	private static Map<String, ChecksumStrategy> createChecksumStrategies() {
		final Map<String, ChecksumStrategy> strategiesByToken = new LinkedHashMap<>();
		registerChecksumStrategy(
						strategiesByToken,
						new ChecksumStrategy(
										"md5",
										HttpHeaderNames.CONTENT_MD5.toString(),
										"MD5",
										"ChecksumMD5",
										null,
										THREAD_LOCAL_MD5::get));
		registerChecksumStrategy(
						strategiesByToken,
						new ChecksumStrategy(
										"crc32",
										S3Api.AMZ_CHECKSUM_PREFIX + "crc32",
										"CRC32",
										"ChecksumCRC32",
										S3Api.AMZ_CHECKSUM_PREFIX + "crc32",
										THREAD_LOCAL_CRC32::get));
		registerChecksumStrategy(
						strategiesByToken,
						new ChecksumStrategy(
										"crc32c",
										S3Api.AMZ_CHECKSUM_PREFIX + "crc32c",
										"CRC32C",
										"ChecksumCRC32C",
										S3Api.AMZ_CHECKSUM_PREFIX + "crc32c",
										THREAD_LOCAL_CRC32C::get));
		registerChecksumStrategy(
						strategiesByToken,
						new ChecksumStrategy(
										"sha1",
										S3Api.AMZ_CHECKSUM_PREFIX + "sha1",
										"SHA1",
										"ChecksumSHA1",
										S3Api.AMZ_CHECKSUM_PREFIX + "sha1",
										THREAD_LOCAL_SHA1::get));
		registerChecksumStrategy(
						strategiesByToken,
						new ChecksumStrategy(
										"sha256",
										S3Api.AMZ_CHECKSUM_PREFIX + "sha256",
										"SHA256",
										"ChecksumSHA256",
										S3Api.AMZ_CHECKSUM_PREFIX + "sha256",
										THREAD_LOCAL_SHA256::get));
		registerChecksumStrategy(
						strategiesByToken,
						new ChecksumStrategy(
										"crc64-nvme",
										S3Api.AMZ_CHECKSUM_PREFIX + "crc64nvme",
										"CRC64NVME",
										"ChecksumCRC64NVME",
										S3Api.AMZ_CHECKSUM_PREFIX + "crc64nvme",
										THREAD_LOCAL_CRC64_NVME::get));
		return Collections.unmodifiableMap(strategiesByToken);
	}

	private static void registerChecksumStrategy(
					final Map<String, ChecksumStrategy> strategiesByToken,
					final ChecksumStrategy checksumStrategy) {
		strategiesByToken.put(checksumStrategy.configToken, checksumStrategy);
	}

	/** The most of an unrecognized status a notice quotes. */
	private static final int MAX_QUOTED_STATUS = 64;

	protected final boolean fsAccess;
	protected final boolean taggingEnabled;
	protected final Input<String> taggingContentInput;
	protected final int authVersion;
	protected final boolean versioning;
	protected final String awsRegion;
	protected final String checksumAlgorithm;
	protected final ChecksumStrategy checksumStrategy;
	protected final String sigV4ServiceName;
	/** The pending aborts that follow failed multipart completions, each with its upload. */
	private final Map<CompletableFuture<Void>, PendingAbort> multipartCleanupTasks = new ConcurrentHashMap<>();
	/**
	 * Per path item input listing, keyed by its path and prefix: the listed keys that are not
	 * generated item ids, summed over its pages and logged once when the listing ends.
	 */
	private final Map<String, NonGeneratedKeys> nonGeneratedKeys = new ConcurrentHashMap<>();
	/** Set by the close before it takes its snapshot; an abort scheduled after that is not sent. */
	private volatile boolean multipartCleanupClosing;
	private volatile long multipartCleanupStopWaitMillis = MULTIPART_CLEANUP_STOP_WAIT_MILLIS;
	private volatile long multipartCleanupCloseCapMillis = MULTIPART_CLEANUP_CLOSE_CAP_MILLIS;
	// One WARN for a multipart abort the kernel route could not connect; the rest go to DEBUG.
	private final AtomicBoolean kernelRouteAbortWarned = new AtomicBoolean();
	private final S3ResponseHandler.CompletionErrorCodeLog completionErrorCodeLog = new S3ResponseHandler.CompletionErrorCodeLog();
	// One WARN for a multipart response body over S3ResponseHandler.MAX_MULTIPART_RESPONSE_BYTES.
	private final AtomicBoolean multipartResponseOverflowWarned = new AtomicBoolean();
	private final KernelRouteAborts<KernelRouteAbort> kernelRouteAborts = new KernelRouteAborts<>();
	// Host of driver-built helper requests (bucket setup, listing, verification, abort).
	private String helperAuthority;

	public S3StorageDriver(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize)
					throws IllegalConfigurationException, InterruptedException {
		this(stepId, itemDataInput, storageConfig, verifyFlag, batchSize, "s3");
	}

	/**
	 * A tag's value as the tagging body takes it. A list or map, which a defaults document can give, is
	 * refused with the tag's name; the {@code s3-aws} driver sends one as text, so the configuration
	 * keeps it. Any other value is cast as before.
	 */
	private static String tagTextOrNull(final String name, final Object value) {
		InvalidTextValueException.refuseListOrMap("storage-object-tagging-tags-" + name, value);
		return (String) value;
	}

	protected S3StorageDriver(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize,
					final String sigV4ServiceName)
					throws IllegalConfigurationException, InterruptedException {
		super(stepId, itemDataInput, storageConfig, verifyFlag, batchSize);
		this.sigV4ServiceName = sigV4ServiceName;
		this.helperAuthority = storageNodeAddrs[0];
		sharedHeaders.remove(HttpHeaderNames.CONNECTION);
		sharedHeaders.remove(HttpHeaderNames.DATE);
		final var objectConfig = storageConfig.configVal("object");
		fsAccess = objectConfig.boolVal("fsAccess");
		final var taggingConfig = objectConfig.configVal("tagging");
		taggingEnabled = taggingConfig.boolVal("enabled");
		if (taggingEnabled) {
			Loggers.MSG.info("{}: S3 Object Tagging Mode Enabled", stepId);
		}
		final var tags = taggingConfig.<Object> mapVal("tags");
		final StringBuilder exprBuff = new StringBuilder();
		exprBuff.append(TAGGING_HEADER);
		for (final var tagEntry : tags.entrySet()) {
			final var k = tagEntry.getKey();
			final var v = tagTextOrNull(k, tagEntry.getValue());
			exprBuff
							.append(TAGGING_ENTRY_START)
							.append(k)
							.append(TAGGING_ENTRY_MIDDLE)
							.append(v)
							.append(TAGGING_ENTRY_END);
		}
		exprBuff.append(TAGGING_FOOTER);
		final var taggingContentExpr = exprBuff.toString();
		if (taggingEnabled) {
			Loggers.MSG.debug("{}: tagging content pattern: {}", stepId, taggingContentExpr);
		}
		if (taggingContentExpr.contains(ASYNC_MARKER) ||
						taggingContentExpr.contains(SYNC_MARKER) ||
						taggingContentExpr.contains(INIT_MARKER)) {
			taggingContentInput = CompositeExpressionInputBuilder
							.newInstance()
							.expression(exprBuff.toString())
							.build();
		} else {
			taggingContentInput = new ConstantValueInputImpl<>(taggingContentExpr);
		}
		versioning = objectConfig.boolVal("versioning");
		if (versioning) {
			Loggers.MSG.info("Versioning is enabled. Make sure that if you use input items list it has version ids");
		}
		var authVersionTemp = storageConfig.intVal("auth-version");
		if ((authVersionTemp != 2) && (authVersionTemp != 4)) {
			throw new AssertionError("Only 2 and 4 versions are supported");
		}
		authVersion = authVersionTemp;
		requestAuthTokenFunc = null; // do not use

		// Validate the checksum algorithm
		if (storageConfig.boolVal("checksum-enabled")) {
			checksumStrategy = CHECKSUM_STRATEGIES.get(storageConfig.stringVal("checksum-algorithm"));
			if (checksumStrategy == null) {
				throw new IllegalArgumentException(
								"Invalid checksum algorithm: " + storageConfig.stringVal("checksum-algorithm"));
			}
			checksumAlgorithm = checksumStrategy.configToken;
			Loggers.MSG.info("Checksum algorithm: {}", checksumAlgorithm);
		} else {
			checksumAlgorithm = null;
			checksumStrategy = null;
		}

		awsRegion = AwsS3Region.effective(
						storageConfig.stringVal("region"),
						storageNodeAddrs.length == 1 ? storageNodeAddrs[0] : null,
						S3StorageDriver::warnRegionConflictOnce);
	}

	private static final Set<String> REGION_CONFLICT_WARNINGS = ConcurrentHashMap.newKeySet();

	private static void warnRegionConflictOnce(final String warning) {
		if (REGION_CONFLICT_WARNINGS.add(warning)) {
			Loggers.MSG.warn(warning);
		}
	}

	/**
	 * The SigV4 signing key of one secret. It keeps the key of the last date it was asked for, so a
	 * secret's key is derived once a day on each thread, whatever other secrets the thread signs with.
	 * Region and service name are the driver's own and never vary, so the secret and the date are
	 * all the key depends on.
	 *
	 * <p>Each instance is reachable only from one thread's {@link #signingKeyCreateFunctionCache}
	 * map, so only that thread reads or writes its date and key. It is static and copies the
	 * driver's region and service name, so a signing thread's cache, which outlives the driver,
	 * keeps no driver reachable.
	 *
	 * <p>It is final, so the signing path's check for it is a single class comparison: only this
	 * function bypasses {@link #signingKeyCache}.
	 */
	static final class SigningKeyCreateFunctionImpl
					implements SigningKeyCreateFunction {

		private final String secret;
		private final String region;
		private final String service;
		private String date;
		private byte[] key;

		SigningKeyCreateFunctionImpl(final String secret, final String region, final String service) {
			this.secret = secret;
			this.region = region;
			this.service = service;
		}

		/**
		 * The signing key for {@code datestamp}, derived only when the date is not the last one. Each
		 * derivation returns a new array, and a returned key is never changed.
		 */
		@Override
		public byte[] apply(final String datestamp) {
			if (!datestamp.equals(date)) {
				key = getSignatureKey(secret, datestamp, region, service);
				date = datestamp;
			}
			return key;
		}
	}

	private static byte[] HmacSHA256(String data, byte[] key) throws NoSuchAlgorithmException, InvalidKeyException {
		Mac mac = Mac.getInstance(S3Api.SIGN_V4_METHOD);
		mac.init(new SecretKeySpec(key, "RawBytes"));
		return mac.doFinal(data.getBytes(UTF_8));
	}

	private static byte[] getSignatureKey(String key, String dateStamp, String region, String service) {
		byte[] kSigning = new byte[0];
		try {
			byte[] kSecret = ("AWS4" + key).getBytes(UTF_8);
			byte[] kDate = HmacSHA256(dateStamp, kSecret);
			byte[] kRegion = HmacSHA256(region, kDate);
			byte[] kService = HmacSHA256(service, kRegion);
			kSigning = HmacSHA256("aws4_request", kService);
			int[] m = new int[100];
			for (int i = 0; i < kSigning.length; i++) {
				m[i] = kSigning[i] & 0xFF;
			}
			Loggers.MSG.trace("sign key", m);
		} catch (NoSuchAlgorithmException | InvalidKeyException e) {
			Loggers.MSG.warn("Couldn't complete HmacSHA256 for auth v4. Either secret is incorrect " +
							"or used algorithm doesn't exist. \n{}", e.getMessage());
		}
		return kSigning;
	}

	@Override
	protected String requestNewPathCacheKey(final String path) {
		final var relPath = path.startsWith(SLASH) ? path.substring(1) : path;
		final var slashPos = relPath.indexOf(SLASH);
		return SLASH + (slashPos > 0 ? relPath.substring(0, slashPos) : relPath);
	}

	/** Replaces the Host of helper requests; for drivers that select destinations per request. */
	protected final void helperAuthority(final String authority) {
		this.helperAuthority = authority;
	}

	/**
	 * Worker-side initialization of a destination bucket, once per bucket per driver instance.
	 *
	 * <p>The controller engine provisions target buckets before any slice starts, so a worker never
	 * creates one. It checks the bucket with {@code HEAD}, which only reports, and reconciles bucket
	 * versioning when that is enabled.
	 */
	@Override
	protected String requestNewPath(final String path) {
		final var bucketPath = requestNewPathCacheKey(path);
		final var uriQuery = uriQuery();
		final var uri = uriQuery == null || uriQuery.isEmpty() ? bucketPath : bucketPath + uriQuery;
		Loggers.MSG.debug(
						"requestNewPath: path={}, bucketPath={}, uri={}",
						path,
						bucketPath,
						uri);
		final var nodeAddr = helperAuthority;
		var reqHeaders = (HttpHeaders) new DefaultHttpHeaders();
		reqHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		applyDynamicHeaders(reqHeaders);
		applySharedHeaders(reqHeaders);
		final var credential = pathToCredMap.getOrDefault(bucketPath, this.credential);
		applyAuthHeaders(reqHeaders, HttpMethod.HEAD, uri, credential);
		if (Loggers.MSG.isDebugEnabled()) {
			for (final var header : reqHeaders) {
				Loggers.MSG.debug(
								"requestNewPath HEAD header {}: {}",
								header.getKey(),
								safeHeaderValueForLogging(header.getKey(), header.getValue()));
			}
		}
		final FullHttpRequest checkBucketReq = new DefaultFullHttpRequest(
						HttpVersion.HTTP_1_1,
						HttpMethod.HEAD,
						uri,
						Unpooled.EMPTY_BUFFER,
						reqHeaders,
						EmptyHttpHeaders.INSTANCE);
		FullHttpResponse checkBucketResp = null;
		try {
			checkBucketResp = executeHttpRequest(checkBucketReq);
		} catch (final InterruptedException e) {
			throwUnchecked(e);
		} catch (final ConnectException e) {
			LogUtil.exception(Level.WARN, e, "Failed to connect to the storage node");
			return null;
		}
		if (checkBucketResp != null) {
			if (!HttpStatusClass.SUCCESS.equals(checkBucketResp.status().codeClass())) {
				Loggers.MSG.debug(
								"{}: the bucket {} check response is {}", stepId, bucketPath, checkBucketResp.status());
			}
			checkBucketResp.release();
		}

		// check the bucket versioning state
		if (versioning) {
			checkBucketVersioning(nodeAddr, bucketPath, credential);
		}
		return path;
	}

	/**
	 * The first-use versioning check of a bucket: {@code GET ?versioning}, then {@link
	 * #handleCheckBucketVersioningResponse(FullHttpResponse, String, String)}, which by default sends
	 * {@code PUT ?versioning} only for a bucket that has never been versioned. Each outcome of the
	 * default handling logs one {@link BucketVersioningNotice}; none throws, so the bucket's path is
	 * cached and never checked again. The check is marked as running until it returns, so the Entry
	 * Node waits for it at the step's stop.
	 */
	private void checkBucketVersioning(final String nodeAddr, final String bucketPath, final Credential credential) {
		try (final var check = BucketVersioningNotice.check(stepId, bucketLabel(bucketPath))) {
			checkBucketVersioningOnce(nodeAddr, bucketPath, credential);
		}
	}

	private void checkBucketVersioningOnce(final String nodeAddr, final String bucketPath, final Credential credential) {
		final var bucketVersioningReqUri = bucketPath + "?" + S3Api.URL_ARG_VERSIONING;
		final FullHttpRequest getBucketVersioningReq = new DefaultFullHttpRequest(
						HttpVersion.HTTP_1_1,
						HttpMethod.GET,
						bucketVersioningReqUri,
						Unpooled.EMPTY_BUFFER,
						bucketVersioningHeaders(nodeAddr, HttpMethod.GET, bucketVersioningReqUri, 0, credential),
						EmptyHttpHeaders.INSTANCE);
		final FullHttpResponse getBucketVersioningResp;
		try {
			getBucketVersioningResp = executeHttpRequest(getBucketVersioningReq);
		} catch (final InterruptedException e) {
			throwUnchecked(e);
			return;
		} catch (final ConnectException e) {
			LogUtil.exception(Level.DEBUG, e, "{}: the bucket {} versioning check failed to connect", stepId, bucketPath);
			BucketVersioningNotice.warn(stepId, versioningCheckFailed(bucketPath, "the connection failed"));
			return;
		}
		if (getBucketVersioningResp == null) {
			BucketVersioningNotice.warn(
							stepId, versioningCheckFailed(bucketPath, "GET ?versioning timed out after " + netTimeoutMilliSec + " ms"));
			return;
		}
		try {
			// The protected hook, so a subclass's override still decides what is done.
			handleCheckBucketVersioningResponse(getBucketVersioningResp, nodeAddr, bucketVersioningReqUri);
		} finally {
			getBucketVersioningResp.release();
		}
	}

	/**
	 * The headers of a versioning request, built as the bucket {@code HEAD} builds its own: the
	 * dynamic and shared headers, and the bucket's credential. A {@code PUT} also gets a {@code Date}
	 * when those headers give none; a {@code GET}, like the {@code HEAD}, does not.
	 */
	private HttpHeaders bucketVersioningHeaders(
					final String nodeAddr,
					final HttpMethod method,
					final String uri,
					final int contentLength,
					final Credential credential) {
		final HttpHeaders reqHeaders = new DefaultHttpHeaders();
		reqHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, contentLength);
		applyDynamicHeaders(reqHeaders);
		applySharedHeaders(reqHeaders);
		if (HttpMethod.PUT.equals(method) && !reqHeaders.contains(HttpHeaderNames.DATE)) {
			reqHeaders.set(HttpHeaderNames.DATE, DateUtil.formatNowRfc1123());
		}
		applyAuthHeaders(reqHeaders, method, uri, credential);
		return reqHeaders;
	}

	/**
	 * Target-bucket provisioning check: {@code HEAD /bucket} with the documented global HTTP
	 * headers and URI arguments, signed with the bucket's credential.
	 *
	 * @param credential the per-path credential, or null for the driver's own
	 */
	public final TargetBucketResponse checkTargetBucket(final String bucket, final Credential credential)
					throws InterruptedException {
		return targetBucketRequest(HttpMethod.HEAD, bucket, new byte[0], credential);
	}

	/**
	 * Target-bucket provisioning creation: {@code PUT /bucket}, with a location constraint only for
	 * a recognized AWS endpoint outside {@code us-east-1}. It carries no object metadata.
	 *
	 * @param credential the per-path credential, or null for the driver's own
	 */
	public final TargetBucketResponse createTargetBucket(final String bucket, final Credential credential)
					throws InterruptedException {
		final var body = AwsS3Region.createBucketBody(awsRegion, helperAuthority);
		return targetBucketRequest(HttpMethod.PUT, bucket, body, credential);
	}

	private TargetBucketResponse targetBucketRequest(
					final HttpMethod method, final String bucket, final byte[] body, final Credential credential)
					throws InterruptedException {
		final var uriQuery = uriQuery();
		final var bucketPath = SLASH + bucket;
		final var uri = uriQuery == null || uriQuery.isEmpty() ? bucketPath : bucketPath + uriQuery;
		final var reqHeaders = new DefaultHttpHeaders();
		reqHeaders.set(HttpHeaderNames.HOST, helperAuthority);
		reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, body.length);
		applyDynamicHeaders(reqHeaders);
		applySharedHeaders(reqHeaders);
		applyAuthHeaders(reqHeaders, method, uri, credential);
		if (Loggers.MSG.isDebugEnabled()) {
			for (final var header : reqHeaders) {
				Loggers.MSG.debug(
								"{}: target bucket {} header {}: {}",
								stepId,
								method,
								header.getKey(),
								safeHeaderValueForLogging(header.getKey(), header.getValue()));
			}
		}
		final FullHttpRequest request = new DefaultFullHttpRequest(
						HttpVersion.HTTP_1_1,
						method,
						uri,
						body.length == 0 ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(body),
						reqHeaders,
						EmptyHttpHeaders.INSTANCE);
		final FullHttpResponse response;
		try {
			response = executeHttpRequest(request, false);
		} catch (final ConnectException e) {
			Loggers.MSG.debug("{}: target bucket {} connection failure: {}", stepId, method, e.toString());
			return TargetBucketResponse.failed("connection failed");
		}
		if (response == null) {
			return TargetBucketResponse.failed("timed out after " + netTimeoutMilliSec + " ms");
		}
		try {
			return TargetBucketResponse.ofHttpStatus(response.status().code());
		} finally {
			response.release();
		}
	}

	static String safeHeaderValueForLogging(final String headerName, final String headerValue) {
		final String normalizedHeaderName = headerName.toLowerCase(Locale.ROOT);
		if (HttpHeaderNames.AUTHORIZATION.contentEqualsIgnoreCase(headerName) ||
						HttpHeaderNames.COOKIE.contentEqualsIgnoreCase(headerName) ||
						"x-amz-security-token".equals(normalizedHeaderName)) {
			return REDACTED_HEADER_VALUE;
		}
		return headerValue;
	}

	/**
	 * Acts on a bucket's {@code GET ?versioning} response, with versioning on; the first-use check
	 * calls it for each bucket. A bucket whose {@code Status} is {@code Enabled} is left as it is.
	 * A {@code VersioningConfiguration} with no or an empty {@code Status}, so a bucket that has
	 * never been versioned, goes to {@link #enableBucketVersioning(String, String)}. A {@code
	 * Suspended} or unrecognized status, a body that isn't a readable {@code
	 * VersioningConfiguration}, and a failed response are left alone with a warning, and the run
	 * goes on un-versioned.
	 *
	 * <p>A subclass that overrides it decides instead, and logs no notice unless it calls this one.
	 */
	protected void handleCheckBucketVersioningResponse(
					final FullHttpResponse getBucketVersioningResp,
					final String nodeAddr,
					final String bucketVersioningReqUri) {
		if (!versioning) {
			return;
		}
		final var bucketPath = bucketPathOf(bucketVersioningReqUri);
		if (!HttpStatusClass.SUCCESS.equals(getBucketVersioningResp.status().codeClass())) {
			BucketVersioningNotice.warn(stepId, versioningCheckFailed(
							bucketPath, "GET ?versioning returned " + getBucketVersioningResp.status()));
			return;
		}
		final var state = versioningState(ByteBufUtil.getBytes(getBucketVersioningResp.content()));
		switch (state.kind()) {
		case NEVER -> enableBucketVersioning(nodeAddr, bucketVersioningReqUri);
		case ENABLED -> BucketVersioningNotice.info(stepId, bucketLabel(bucketPath) + ": versioning is already enabled");
		case SUSPENDED -> BucketVersioningNotice.warn(stepId, bucketLabel(bucketPath)
						+ ": versioning is suspended; SPT left it alone, and the run goes on un-versioned");
		case OTHER -> BucketVersioningNotice.warn(stepId, bucketLabel(bucketPath) + ": the versioning status \""
						+ quotedStatus(state.status())
						+ "\" is neither Enabled nor Suspended; SPT left it alone, and the run goes on un-versioned");
		default -> BucketVersioningNotice.warn(stepId, versioningCheckFailed(
						bucketPath, "GET ?versioning returned a body that is not a VersioningConfiguration SPT can read"));
		}
	}

	/**
	 * Enables a never-versioned bucket's versioning; {@link
	 * #handleCheckBucketVersioningResponse(FullHttpResponse, String, String)} calls it. By default it
	 * sends {@code PUT ?versioning} signed with the bucket's credential from {@link #pathToCredMap},
	 * the one the first-use check used. A subclass that overrides it decides whether anything is sent,
	 * and logs no notice unless it calls this one.
	 */
	protected void enableBucketVersioning(
					final String nodeAddr, final String bucketVersioningReqUri) {
		enableBucketVersioning(nodeAddr, bucketVersioningReqUri, bucketCredential(bucketVersioningReqUri));
	}

	/**
	 * Sends {@code PUT ?versioning} to enable a bucket's versioning, with the headers and credential
	 * of the {@code GET ?versioning}, and logs what came of it. A failure, including a timeout, is a
	 * warning, and the run goes on.
	 *
	 * @param credential the bucket's credential, or null for the driver's own
	 */
	protected void enableBucketVersioning(
					final String nodeAddr, final String bucketVersioningReqUri, final Credential credential) {
		final var bucketPath = bucketPathOf(bucketVersioningReqUri);
		final var putBucketVersioningReq = (FullHttpRequest) new DefaultFullHttpRequest(
						HttpVersion.HTTP_1_1,
						HttpMethod.PUT,
						bucketVersioningReqUri,
						Unpooled.wrappedBuffer(S3Api.VERSIONING_ENABLE_CONTENT).retain(),
						bucketVersioningHeaders(nodeAddr, HttpMethod.PUT, bucketVersioningReqUri,
										S3Api.VERSIONING_ENABLE_CONTENT.length, credential),
						EmptyHttpHeaders.INSTANCE);
		final FullHttpResponse putBucketVersioningResp;
		try {
			putBucketVersioningResp = executeHttpRequest(putBucketVersioningReq);
		} catch (final InterruptedException e) {
			throwUnchecked(e);
			return;
		} catch (final ConnectException e) {
			LogUtil.exception(Level.DEBUG, e, "{}: enabling the bucket {} versioning failed to connect", stepId, bucketPath);
			BucketVersioningNotice.warn(stepId, versioningNotEnabled(bucketPath, "the connection failed"));
			return;
		}
		if (putBucketVersioningResp == null) {
			BucketVersioningNotice.warn(stepId, versioningNotEnabled(
							bucketPath, "PUT ?versioning timed out after " + netTimeoutMilliSec + " ms"));
			return;
		}
		try {
			if (HttpStatusClass.SUCCESS.equals(putBucketVersioningResp.status().codeClass())) {
				// A WARN (owner, 2026-11-15): the run changed the bucket, so it is reported at any level up to WARN.
				BucketVersioningNotice.warn(stepId, bucketLabel(bucketPath)
								+ ": versioning enabled by this run (the bucket had never been versioned)");
			} else {
				BucketVersioningNotice.warn(stepId, versioningNotEnabled(
								bucketPath, "PUT ?versioning returned " + putBucketVersioningResp.status()));
			}
		} finally {
			putBucketVersioningResp.release();
		}
	}

	/**
	 * What a {@code GET ?versioning} body says, read by a hardened namespace-aware parser: only a
	 * {@code VersioningConfiguration} with no or an empty {@code Status} counts as never versioned.
	 */
	static VersioningConfigurationXmlHandler.State versioningState(final byte[] body) {
		return VersioningConfigurationXmlHandler.read(body);
	}

	/** A status for a notice: on one line, at most {@value #MAX_QUOTED_STATUS} characters. */
	static String quotedStatus(final String status) {
		final var line = BucketVersioningNotice.oneLine(status).replaceAll("\\s+", " ");
		return line.length() <= MAX_QUOTED_STATUS ? line : line.substring(0, MAX_QUOTED_STATUS) + "...";
	}

	private Credential bucketCredential(final String bucketVersioningReqUri) {
		return pathToCredMap.getOrDefault(bucketPathOf(bucketVersioningReqUri), credential);
	}

	private static String bucketPathOf(final String bucketVersioningReqUri) {
		final var query = bucketVersioningReqUri.indexOf('?');
		return query < 0 ? bucketVersioningReqUri : bucketVersioningReqUri.substring(0, query);
	}

	private static String bucketLabel(final String bucketPath) {
		return "bucket " + (bucketPath.startsWith(SLASH) ? bucketPath.substring(1) : bucketPath);
	}

	private static String versioningCheckFailed(final String bucketPath, final String cause) {
		return bucketLabel(bucketPath) + ": could not read the versioning state (" + cause
						+ "); SPT sent no request to enable it, and the run goes on un-versioned";
	}

	private static String versioningNotEnabled(final String bucketPath, final String cause) {
		return bucketLabel(bucketPath) + ": could not enable versioning (" + cause + "); the run goes on un-versioned";
	}

	@Override
	protected final String requestNewAuthToken(final Credential credential) {
		throw new AssertionError("Should not be invoked");
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
		return list(itemFactory, path, prefix, idRadix, lastPrevItem, count, ListOptions.DEFAULT);
	}

	@Override
	public com.dell.spt.base.storage.driver.ListDiscoveryProbe.DiscoverResult probeCommonPrefixes(
					final String bucketPath,
					final String prefix,
					final String delimiter,
					final int maxKeys) throws IOException {
		final String canonicalPrefix = sanitizePrefix(prefix);
		final var nodeAddr = helperAuthority;
		final var reqHeaders = new DefaultHttpHeaders();
		reqHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		applyDynamicHeaders(reqHeaders);
		applySharedHeaders(reqHeaders);
		final var uriBuilder = BUCKET_LIST_QUERY.get();
		uriBuilder.setLength(0);
		uriBuilder.append(bucketPath);
		appendInitialQuery(uriBuilder, "list-type=2");
		appendQueryParam(uriBuilder, "prefix", canonicalPrefix);
		appendQueryParam(uriBuilder, "delimiter", delimiter);
		appendQueryParam(uriBuilder, "max-keys",
						Integer.toString(Math.min(Math.max(maxKeys, 1), S3Api.MAX_KEYS_LIMIT)));
		final var uri = uriBuilder.toString();
		applyAuthHeaders(reqHeaders, HttpMethod.GET, uri, credential);
		final FullHttpRequest req = new DefaultFullHttpRequest(
						HttpVersion.HTTP_1_1, HttpMethod.GET, uri, Unpooled.EMPTY_BUFFER,
						reqHeaders, EmptyHttpHeaders.INSTANCE);
		try {
			final var resp = executeHttpRequest(req);
			try {
				if (resp == null) {
					throw new IOException("S3 LIST delimiter probe returned no response");
				}
				if (!HttpStatusClass.SUCCESS.equals(resp.status().codeClass())) {
					throw new IOException("S3 LIST delimiter probe failed: " + resp.status());
				}
				final var content = resp.content();
				final var handler = new CommonPrefixesXmlHandler();
				try (final InputStream is = new ByteBufInputStream(content)) {
					S3XmlParser.parse(is, handler);
				}
				return new com.dell.spt.base.storage.driver.ListDiscoveryProbe.DiscoverResult(
								java.util.List.copyOf(handler.commonPrefixes()),
								handler.hasContents(), handler.truncated());
			} finally {
				if (resp != null) {
					resp.release();
				}
			}
		} catch (final InterruptedException e) {
			throwUnchecked(e);
			throw new IOException("S3 LIST delimiter probe interrupted", e);
		} catch (final SAXException e) {
			throw new IOException("Failed to parse the S3 LIST delimiter probe", e);
		} catch (final ParserConfigurationException e) {
			throw new IOException("Failed to initialize the S3 LIST response parser", e);
		} catch (final ConnectException e) {
			throw new IOException("Failed to connect for S3 LIST delimiter probe", e);
		}
	}

	@Override
	public List<I> list(
					final ItemFactory<I> itemFactory,
					final String path,
					final String prefix,
					final int idRadix,
					final I lastPrevItem,
					final int count,
					final ListOptions options)
					throws IOException {
		final String canonicalPrefix = sanitizePrefix(prefix);
		final var requestedMax = options.maxKeys() > 0 ? options.maxKeys() : S3Api.MAX_KEYS_LIMIT;
		final var countLimit = Math.min(
						Math.max(count, 1), Math.min(requestedMax, S3Api.MAX_KEYS_LIMIT));
		final var nodeAddr = helperAuthority;
		final var reqHeaders = new DefaultHttpHeaders();
		reqHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		applyDynamicHeaders(reqHeaders);
		applySharedHeaders(reqHeaders);
		final var uriBuilder = BUCKET_LIST_QUERY.get();
		uriBuilder.setLength(0);
		uriBuilder.append(path);
		if (options.includeVersions()) {
			appendInitialQuery(uriBuilder, "versions");
			appendQueryParam(uriBuilder, "prefix", canonicalPrefix);
			appendQueryParam(uriBuilder, "delimiter", options.delimiter());
			appendQueryParam(uriBuilder, "key-marker", sanitizePrefix(options.keyMarker()));
			appendQueryParam(uriBuilder, "version-id-marker", options.versionIdMarker());
		} else {
			appendInitialQuery(uriBuilder, "list-type=2");
			appendQueryParam(uriBuilder, "prefix", canonicalPrefix);
			appendQueryParam(uriBuilder, "delimiter", options.delimiter());
			if (options.continuationToken() != null && !options.continuationToken().isEmpty()) {
				appendQueryParam(uriBuilder, "continuation-token", options.continuationToken());
			} else if (options.startAfter() != null && !options.startAfter().isEmpty()) {
				appendQueryParam(uriBuilder, "start-after", sanitizePrefix(options.startAfter()));
			} else if (lastPrevItem != null) {
				appendQueryParam(uriBuilder, "start-after", listedKey(lastPrevItem.name(), path));
			}
		}
		appendQueryParam(uriBuilder, "max-keys", Integer.toString(countLimit));
		final var uri = uriBuilder.toString();
		applyAuthHeaders(reqHeaders, HttpMethod.GET, uri, credential);
		final FullHttpRequest checkBucketReq = new DefaultFullHttpRequest(
						HttpVersion.HTTP_1_1, HttpMethod.GET, uri, Unpooled.EMPTY_BUFFER,
						reqHeaders, EmptyHttpHeaders.INSTANCE);
		final List<I> buff = new ArrayList<>(countLimit);
		try {
			final var listResp = executeHttpRequest(checkBucketReq);
			try {
				if (listResp == null) {
					throw new IOException("S3 LIST returned no response");
				}
				if (!HttpStatusClass.SUCCESS.equals(listResp.status().codeClass())) {
					throw new IOException("S3 LIST failed: " + listResp.status());
				}
				final var listRespContent = listResp.content();
				final var listingHandler = new BucketXmlListingHandler<>(
								buff, path, canonicalPrefix, itemFactory, idRadix, verifyFlag);
				try (final InputStream contentStream = new ByteBufInputStream(listRespContent)) {
					S3XmlParser.parse(contentStream, listingHandler);
				}
				countNonGeneratedKeys(
								path, canonicalPrefix,
								lastPrevItem == null && !options.includeVersions()
												&& isNullOrEmpty(options.continuationToken())
												&& isNullOrEmpty(options.startAfter()),
								listingHandler.firstNonGeneratedKey(), listingHandler.nonGeneratedKeyCount(),
								!listingHandler.isTruncated());
				if (buff.isEmpty()) {
					throw new EOFException();
				}
				if (!listingHandler.isTruncated()) {
					buff.add(null); // poison
				}
			} finally {
				if (listResp != null) {
					listResp.release();
				}
			}
		} catch (final InterruptedException e) {
			throwUnchecked(e);
			throw new IOException("S3 LIST interrupted", e);
		} catch (final SAXException e) {
			throw deterministicListInputFailure("Failed to parse the S3 object listing", e);
		} catch (final ParserConfigurationException e) {
			throw deterministicListInputFailure("Failed to initialize the S3 LIST response parser", e);
		} catch (final ConnectException e) {
			throw deterministicListInputFailure("Failed to connect for S3 LIST", e);
		} catch (final EOFException e) {
			throw e;
		} catch (final IOException e) {
			throw deterministicListInputFailure("S3 LIST input failed", e);
		}
		return buff;
	}

	@Override
	protected HttpRequest httpRequest(final O op, final String nodeAddr) throws URISyntaxException {
		final HttpRequest httpRequest;
		final OpType opType = op.type();
		if (op instanceof DeleteRequestOperation deleteOperation) {
			httpRequest = standaloneDeleteRequest(deleteOperation, nodeAddr);
		} else if (OpType.DELETE.equals(opType) && op.requestedVersionId() != null) {
			// A manifest VersionedItem is an explicit object identity. Preserve it before
			// tagging or composite-object dispatch can reinterpret the DELETE request.
			httpRequest = objectVersioningRequest(op, nodeAddr);
		} else if (op instanceof CompositeDataOperation) {
			if (OpType.CREATE.equals(opType)) {
				final var mpuOp = (CompositeDataOperation) op;
				if (mpuOp.get(S3Api.KEY_MPU_ABORT) != null) {
					httpRequest = abortMultipartUploadRequest(mpuOp, nodeAddr);
				} else if (mpuOp.allSubOperationsDone()) {
					httpRequest = completeMultipartUploadRequest(mpuOp, nodeAddr);
				} else { // this is the initial state of the task
					httpRequest = initMultipartUploadRequest(op, nodeAddr);
				}
			} else if (OpType.READ.equals(opType)) {
				// Composite READs are dispatched as range-GETs by submit(), never as a single HTTP request
				throw new AssertionError("Composite READ must be handled by submit(), not httpRequest()");
			} else {
				// Composite DELETE/UPDATE: the item happens to be composite (has data ranges) but
				// the operation itself is a plain DELETE/UPDATE — delegate to the standard path.
				httpRequest = ordinaryObjectRequest(op, nodeAddr);
			}
		} else if (op instanceof PartialDataOperation) {
			if (OpType.CREATE.equals(opType)) {
				httpRequest = partUploadRequest((PartialDataOperation) op, nodeAddr);
			} else if (OpType.READ.equals(opType)) {
				httpRequest = rangeReadRequest((PartialDataOperation) op, nodeAddr);
			} else {
				throw new AssertionError("Partial " + opType + " operations are not implemented");
			}
		} else if (op instanceof ListOperation) {
			httpRequest = listRequest((ListOperation<?>) op, nodeAddr);
		} else if (taggingEnabled) {
			httpRequest = objectTaggingRequest(op, nodeAddr);
		} else if (versioning
						|| (integrityMetadataEnabled() && op.requestedVersionId() != null)) {
			httpRequest = objectVersioningRequest(op, nodeAddr);
		} else {
			httpRequest = ordinaryObjectRequest(op, nodeAddr);
		}
		return httpRequest;
	}

	/**
	 * Builds the established single-object request path and fails closed if explicit standalone
	 * dispatch was ever bypassed.
	 */
	protected HttpRequest ordinaryObjectRequest(final O op, final String nodeAddr)
					throws URISyntaxException {
		if (op instanceof DeleteRequestOperation deleteOperation) {
			if (deleteOperation.deleteResult() == null) {
				deleteOperation.completeDelete(DeleteTransportResult.representativeItemFallthrough());
			}
			throw new IllegalArgumentException(
							"Standalone DELETE requests cannot enter the representative-item request builder");
		}
		return super.httpRequest(op, nodeAddr);
	}

	private HttpRequest standaloneDeleteRequest(
					final DeleteRequestOperation operation, final String nodeAddr) {
		final var deleteRequest = operation.deleteRequest();
		if (deleteRequest.targets().size() > 1) {
			return standaloneMultiDeleteRequest(operation, nodeAddr);
		}
		final var target = deleteRequest.targets().get(0);
		final var objectUri = standaloneObjectUri(deleteRequest.bucket(), target);
		final var encodedVersionId = target.versionId() == null ? null : encodeVersionId(target.versionId());
		final var uri = encodedVersionId == null ? objectUri : objectUri + VERSION_ID_QUERY + encodedVersionId;
		final HttpHeaders headers = new DefaultHttpHeaders();
		if (nodeAddr != null) {
			headers.set(HttpHeaderNames.HOST, nodeAddr);
		}
		headers.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		applyDynamicHeaders(headers);
		applySharedHeaders(headers);
		applyAuthHeaders(headers, HttpMethod.DELETE, uri, deleteRequest.credential());
		return new DefaultHttpRequest(HTTP_1_1, HttpMethod.DELETE, uri, headers);
	}

	private FullHttpRequest standaloneMultiDeleteRequest(
					final DeleteRequestOperation operation, final String nodeAddr) {
		final var deleteRequest = operation.deleteRequest();
		final var xml = THREAD_LOCAL_STRB.get();
		final byte[] content;
		xml.setLength(0);
		try {
			xml.append(DELETE_REQUEST_HEADER);
			for (final var target : deleteRequest.targets()) {
				xml.append(DELETE_REQUEST_OBJECT_START);
				appendXmlText(xml, target.key());
				xml.append(DELETE_REQUEST_KEY_END);
				if (target.versionId() != null) {
					xml.append(DELETE_REQUEST_VERSION_START);
					appendXmlText(xml, target.versionId());
					xml.append(DELETE_REQUEST_VERSION_END);
				}
				xml.append(DELETE_REQUEST_OBJECT_END);
			}
			xml.append(DELETE_REQUEST_FOOTER);
			content = xml.toString().getBytes(UTF_8);
		} finally {
			xml.setLength(0);
		}
		final var uri = SLASH + deleteRequest.bucket() + '?' + DELETE_QUERY;
		final HttpHeaders headers = new DefaultHttpHeaders();
		if (nodeAddr != null) {
			headers.set(HttpHeaderNames.HOST, nodeAddr);
		}
		headers.set(HttpHeaderNames.CONTENT_LENGTH, content.length);
		headers.set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_XML);
		headers.set(
						HttpHeaderNames.CONTENT_MD5,
						BASE64_ENCODER.encodeToString(THREAD_LOCAL_MD5.get().digest(content)));
		applyDynamicHeaders(headers);
		applySharedHeaders(headers);
		applyAuthHeaders(headers, HttpMethod.POST, uri, deleteRequest.credential());
		return new DefaultFullHttpRequest(
						HTTP_1_1,
						HttpMethod.POST,
						uri,
						Unpooled.wrappedBuffer(content),
						headers,
						EmptyHttpHeaders.INSTANCE);
	}

	private static String standaloneObjectUri(final String bucket, final DeleteTarget target) {
		final var uri = THREAD_LOCAL_STRB.get();
		uri.setLength(0);
		try {
			uri.append(SLASH).append(bucket).append(SLASH);
			appendPercentEncodedPath(uri, target.key());
			return uri.toString();
		} finally {
			uri.setLength(0);
		}
	}

	/** Uses the ordinary HTTP HEAD path for both Netty S3 and its RDMA transport subclass. */
	@Override
	public Presence presence(final DeleteTarget target) {
		final String objectUri = standaloneObjectUri(target.bucket(), target);
		final String encodedVersionId = target.versionId() == null ? null : encodeVersionId(target.versionId());
		final String uri = encodedVersionId == null ? objectUri : objectUri + VERSION_ID_QUERY + encodedVersionId;
		final HttpHeaders headers = new DefaultHttpHeaders();
		final String nodeAddr = helperAuthority;
		headers.set(HttpHeaderNames.HOST, nodeAddr);
		headers.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		applyDynamicHeaders(headers);
		applySharedHeaders(headers);
		final String bucketPath = SLASH + target.bucket();
		applyAuthHeaders(
						headers,
						HttpMethod.HEAD,
						uri,
						pathToCredMap.getOrDefault(bucketPath, this.credential));
		final FullHttpRequest request = new DefaultFullHttpRequest(
						HTTP_1_1,
						HttpMethod.HEAD,
						uri,
						Unpooled.EMPTY_BUFFER,
						headers,
						EmptyHttpHeaders.INSTANCE);
		FullHttpResponse response = null;
		try {
			response = executeHttpRequest(request, false);
			if (response == null) {
				return Presence.UNRESOLVED;
			}
			if (response.status().code() == 404) {
				return Presence.ABSENT;
			}
			return HttpStatusClass.SUCCESS.equals(response.status().codeClass())
							? Presence.PRESENT
							: Presence.UNRESOLVED;
		} catch (final InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			throwUnchecked(interrupted);
			return Presence.UNRESOLVED;
		} catch (final ConnectException failure) {
			Loggers.MSG.debug("DELETE verification HEAD was unresolved: {}", failure.toString());
			return Presence.UNRESOLVED;
		} finally {
			if (response != null) {
				response.release();
			}
		}
	}

	private static void appendPercentEncodedPath(final StringBuilder out, final String value) {
		final byte[] bytes = value.getBytes(UTF_8);
		for (final byte b : bytes) {
			final int ch = b & 0xFF;
			if (isUnreserved(ch) || ch == '/') {
				out.append((char) ch);
			} else {
				out.append('%')
								.append(URI_HEX_DIGITS[(ch >>> 4) & 0xF])
								.append(URI_HEX_DIGITS[ch & 0xF]);
			}
		}
	}

	private static void appendXmlText(final StringBuilder escaped, final String value) {
		for (int i = 0; i < value.length(); i++) {
			final char ch = value.charAt(i);
			switch (ch) {
			case '&':
				escaped.append("&amp;");
				break;
			case '<':
				escaped.append("&lt;");
				break;
			case '>':
				escaped.append("&gt;");
				break;
			case '\"':
				escaped.append("&quot;");
				break;
			case '\'':
				escaped.append("&apos;");
				break;
			case '\r':
				escaped.append("&#13;");
				break;
			default:
				if (ch < 0x20 && ch != '\n' && ch != '\t') {
					throw new IllegalArgumentException(
									"S3 DELETE identity contains a character forbidden by XML 1.0");
				}
				escaped.append(ch);
			}
		}
	}

	private HttpRequest listRequest(final ListOperation<?> op, final String nodeAddr) {
		final var options = op.options();
		final var reqHeaders = new DefaultHttpHeaders();
		reqHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		applyDynamicHeaders(reqHeaders);
		applySharedHeaders(reqHeaders);
		final var uriBuilder = BUCKET_LIST_QUERY.get();
		uriBuilder.setLength(0);
		final var bucketPath = normalizeBucketPath(op.srcPath());
		uriBuilder.append(bucketPath);
		if (options.includeVersions()) {
			appendInitialQuery(uriBuilder, "versions");
		} else {
			appendInitialQuery(uriBuilder, "list-type=2");
		}
		appendQueryParam(uriBuilder, "prefix", sanitizePrefix(op.item().name()));
		appendQueryParam(uriBuilder, "delimiter", options.delimiter());
		final var maxKeys = options.maxKeys() > 0 ? options.maxKeys() : S3Api.MAX_KEYS_LIMIT;
		appendQueryParam(uriBuilder, "max-keys", Integer.toString(Math.min(maxKeys, S3Api.MAX_KEYS_LIMIT)));
		if (options.includeVersions()) {
			appendQueryParam(uriBuilder, "key-marker", options.keyMarker());
			appendQueryParam(uriBuilder, "version-id-marker", options.versionIdMarker());
		} else {
			appendQueryParam(uriBuilder, "start-after", options.startAfter());
			appendQueryParam(uriBuilder, "continuation-token", options.continuationToken());
		}
		final var uri = uriBuilder.toString();
		applyAuthHeaders(reqHeaders, HttpMethod.GET, uri, op.credential());
		Loggers.MSG.debug("LIST request uri={}", uri);
		return new DefaultFullHttpRequest(
						HttpVersion.HTTP_1_1,
						HttpMethod.GET,
						uri,
						Unpooled.EMPTY_BUFFER,
						reqHeaders,
						EmptyHttpHeaders.INSTANCE);
	}

	private static void appendInitialQuery(final StringBuilder uriBuilder, final String initial) {
		if (initial == null || initial.isEmpty()) {
			return;
		}
		final int questionMarkIdx = uriBuilder.indexOf("?");
		if (questionMarkIdx < 0) {
			uriBuilder.append('?');
		} else if (uriBuilder.charAt(uriBuilder.length() - 1) != '?' && uriBuilder.charAt(uriBuilder.length() - 1) != '&') {
			uriBuilder.append('&');
		}
		final int equalsIdx = initial.indexOf('=');
		if (equalsIdx < 0) {
			uriBuilder.append(percentEncode(initial)).append('=');
		} else {
			final var key = initial.substring(0, equalsIdx);
			final var value = initial.substring(equalsIdx + 1);
			uriBuilder.append(percentEncode(key)).append('=').append(percentEncode(value));
		}
	}

	private static void appendQueryParam(final StringBuilder uriBuilder, final String key, final String value) {
		if (key == null || key.isEmpty() || value == null || value.isEmpty()) {
			return;
		}
		if (uriBuilder.indexOf("?") < 0) {
			uriBuilder.append('?');
		} else if (uriBuilder.charAt(uriBuilder.length() - 1) != '?' && uriBuilder.charAt(uriBuilder.length() - 1) != '&') {
			uriBuilder.append('&');
		}
		uriBuilder.append(percentEncode(key)).append('=').append(percentEncode(value));
	}

	/**
	 * The SigV4 order of encoded {@code name=value} pairs: char by char, with {@code '='} below every other
	 * character, and a string that is a prefix of the other first. Each name and value has been through
	 * {@link #percentEncode}, so the pair is ASCII and holds {@code '='} only as its one delimiter, and this
	 * order is the order by encoded name, then by encoded value, that SigV4 asks for.
	 */
	static final Comparator<String> CANONICAL_PAIR_ORDER = new CanonicalPairOrder();

	private static final class CanonicalPairOrder implements Comparator<String> {

		@Override
		public int compare(final String left, final String right) {
			final int common = Math.min(left.length(), right.length());
			for (int i = 0; i < common; i++) {
				final char l = left.charAt(i);
				final char r = right.charAt(i);
				if (l != r) {
					return rank(l) - rank(r);
				}
			}
			return left.length() - right.length();
		}

		private static int rank(final char ch) {
			return ch == '=' ? -1 : ch;
		}
	}

	/**
	 * The V4 canonical form of a non-empty {@code rawQuery}: each pair decoded, encoded again and sorted.
	 * {@link #getCanonicalV4} appends a query that {@link #isCanonicalSingleParameter} accepts as it is,
	 * without calling this.
	 */
	private static String canonicalizeQueryParameters(final String rawQuery) {
		final var parts = rawQuery.split("&", 0);
		final List<String> canonicalComponents = new ArrayList<>(parts.length);
		for (final var part : parts) {
			if (part == null || part.isEmpty()) {
				continue;
			}
			final int eqIdx = part.indexOf('=');
			final String rawKey = eqIdx >= 0 ? part.substring(0, eqIdx) : part;
			final String rawValue = eqIdx >= 0 ? part.substring(eqIdx + 1) : "";
			final String encodedPair = percentEncode(decodeQueryComponent(rawKey)) + '=' + percentEncode(decodeQueryComponent(rawValue));
			canonicalComponents.add(encodedPair);
		}
		canonicalComponents.sort(CANONICAL_PAIR_ORDER);
		return String.join("&", canonicalComponents);
	}

	/**
	 * Whether {@code rawQuery} from {@code start} to its end is one {@code key=value} pair of unreserved
	 * characters, such as {@code versionId=<an AWS version ID>}: decoding and encoding it again gives it
	 * back, and one pair needs no sorting, so it is already canonical. The scan stops at the first other
	 * character.
	 */
	private static boolean isCanonicalSingleParameter(final String rawQuery, final int start) {
		int equalsIndex = -1;
		for (int i = start; i < rawQuery.length(); i++) {
			final char ch = rawQuery.charAt(i);
			if (ch == '=') {
				if (equalsIndex != -1) {
					return false;
				}
				equalsIndex = i;
			} else if (!isUnreserved(ch)) {
				return false;
			}
		}
		return equalsIndex > start;
	}

	private static String normalizeHeaderValue(final String value) {
		if (value == null) {
			return "";
		}
		final var trimmed = value.trim();
		final int len = trimmed.length();
		if (len == 0) {
			return "";
		}
		// Fast path: check if any consecutive whitespace needs collapsing
		boolean needsNormalization = false;
		for (int i = 1; i < len; i++) {
			if (Character.isWhitespace(trimmed.charAt(i)) && Character.isWhitespace(trimmed.charAt(i - 1))) {
				needsNormalization = true;
				break;
			}
		}
		if (!needsNormalization) {
			return trimmed;
		}
		// Collapse runs of whitespace into a single space
		final var sb = new StringBuilder(len);
		boolean prevWs = false;
		for (int i = 0; i < len; i++) {
			final char c = trimmed.charAt(i);
			if (Character.isWhitespace(c)) {
				if (!prevWs) {
					sb.append(' ');
					prevWs = true;
				}
			} else {
				sb.append(c);
				prevWs = false;
			}
		}
		return sb.toString();
	}

	private static String decodeQueryComponent(final String value) {
		if (value == null || value.isEmpty()) {
			return "";
		}
		try {
			return URLDecoder.decode(value.replace("+", "%2B"), UTF_8);
		} catch (final IllegalArgumentException ex) {
			Loggers.MSG.debug("Failed to URL-decode component '{}': {}", value, ex.getMessage());
			return value;
		}
	}

	private static final String VERSION_ID_QUERY = "?versionId=";

	/**
	 * The {@code versionId} query value: {@code versionId} itself when every character is unreserved,
	 * as in an AWS version ID, so the common case allocates nothing; otherwise its percent-encoding.
	 */
	private static String encodeVersionId(final String versionId) {
		for (int i = 0; i < versionId.length(); i++) {
			if (!isUnreserved(versionId.charAt(i))) {
				return percentEncode(versionId);
			}
		}
		return versionId;
	}

	private static String percentEncode(final String value) {
		if (value == null || value.isEmpty()) {
			return "";
		}
		final byte[] bytes = value.getBytes(UTF_8);
		final StringBuilder out = new StringBuilder(bytes.length * 3);
		for (final byte b : bytes) {
			final int ch = b & 0xFF;
			if (isUnreserved(ch)) {
				out.append((char) ch);
			} else {
				out.append('%')
								.append(URI_HEX_DIGITS[(ch >>> 4) & 0xF])
								.append(URI_HEX_DIGITS[ch & 0xF]);
			}
		}
		return out.toString();
	}

	private static boolean isUnreserved(final int ch) {
		return (ch >= '0' && ch <= '9')
						|| (ch >= 'A' && ch <= 'Z')
						|| (ch >= 'a' && ch <= 'z')
						|| ch == '-' || ch == '_' || ch == '.' || ch == '~';
	}

	/** The listed keys of one path item input listing that are not generated item ids. */
	private static final class NonGeneratedKeys {
		private final String label;
		private final String firstKey;
		private long count;

		private NonGeneratedKeys(final String label, final String firstKey) {
			this.label = label;
			this.firstKey = firstKey;
		}
	}

	/**
	 * Sums one listing page's keys that are not generated item ids into its listing's count, and
	 * logs the listing's one summary when the listing ends. A first page starts a new listing; the
	 * summary of one that never reached its last page is logged then, or when the driver closes.
	 */
	private void countNonGeneratedKeys(
					final String path, final String prefix, final boolean firstPage,
					final String firstKeyOnPage, final int countOnPage, final boolean lastPage) {
		final String listing = path + '\n' + (prefix == null ? "" : prefix);
		if (firstPage) {
			logNonGeneratedKeys(nonGeneratedKeys.remove(listing));
		}
		if (countOnPage > 0) {
			final String label = bucketLabel(path) + (prefix == null ? "" : ", prefix \"" + prefix + "\"");
			nonGeneratedKeys.computeIfAbsent(listing, key -> new NonGeneratedKeys(label, firstKeyOnPage)).count += countOnPage;
		}
		if (lastPage) {
			logNonGeneratedKeys(nonGeneratedKeys.remove(listing));
		}
	}

	private void logNonGeneratedKeys(final NonGeneratedKeys keys) {
		if (keys != null) {
			Loggers.MSG.warn(
							"{}: {}: {} listed {} (first \"{}\"); the path item input uses {} with data offset 0",
							stepId, keys.label, keys.count,
							keys.count == 1
											? "key is not an SPT-generated item id"
											: "keys are not SPT-generated item ids",
							keys.firstKey, keys.count == 1 ? "it" : "them");
		}
	}

	private static boolean isNullOrEmpty(final String value) {
		return value == null || value.isEmpty();
	}

	private IOException deterministicListInputFailure(
					final String message, final Throwable cause) {
		if (verifyFlag) {
			return new TerminalItemInputException(message, cause);
		}
		return cause instanceof IOException
						? (IOException) cause
						: new IOException(message, cause);
	}

	private static String normalizeBucketPath(final String path) {
		if (path == null || path.isEmpty()) {
			return SLASH;
		}
		return path.startsWith(SLASH) ? path : SLASH + path;
	}

	private static String sanitizePrefix(final String prefix) {
		if (prefix == null || prefix.isEmpty()) {
			return null;
		}
		return prefix.startsWith(SLASH) ? prefix.substring(1) : prefix;
	}

	/**
	 * Returns the object key a listed item stands for: its name without the listed path. Listed names
	 * are the path, then the key, so sending the whole name as {@code start-after} would name no key
	 * in the bucket. The key is sent whole, since a key may itself start with a slash. A name that
	 * does not hold the path, such as one an extension built, is sent as before.
	 */
	private static String listedKey(final String itemName, final String path) {
		if (path != null && ItemNamePaths.holdsPath(itemName, path)) {
			return ItemNamePaths.relativeToPath(itemName, path);
		}
		return sanitizePrefix(itemName);
	}

	@Override
	protected final HttpMethod tokenHttpMethod(final OpType opType) {
		throw new AssertionError("Not implemented yet");
	}

	@Override
	protected final HttpMethod pathHttpMethod(final OpType opType) {
		switch (opType) {
		case UPDATE:
			throw new AssertionError("Not implemented yet");
		case READ:
			return HttpMethod.GET;
		case STAT:
			return HttpMethod.HEAD;
		case LIST:
			return HttpMethod.GET;
		case DELETE:
			return DELETE;
		default:
			return HttpMethod.PUT;
		}
	}

	@Override
	protected final String tokenUriPath(
					final I item, final String srcPath, final String dstPath, final OpType opType) {
		throw new AssertionError("Not implemented");
	}

	@Override
	protected final String pathUriPath(
					final I item, final String srcPath, final String dstPath, final OpType opType) {
		final var itemName = item.name();
		if (itemName.startsWith(SLASH)) {
			return itemName;
		} else {
			return SLASH + itemName;
		}
	}

	// TODO Handle objectTaggingRequest()
	// TODO Handle other areas where applyAuthHeaders() is called
	@Override
	protected void applyChecksum(final HttpHeaders httpHeaders, final O op) {
		if (checksumStrategy == null || !(op.item() instanceof DataItem)) {
			return;
		}
		if (OpType.CREATE.equals(op.type())
						&& op.integrityMetadata() != null
						&& IntegrityMetadataCodec.ALGORITHM_SHA256.equals(checksumStrategy.configToken)
						&& !(op instanceof PartialDataOperation)) {
			final byte[] digestBytes = HexFormat.of().parseHex(op.integrityMetadata().digest());
			httpHeaders.set(
							checksumStrategy.requestHeaderName, BASE64_ENCODER.encodeToString(digestBytes));
			return;
		}
		var dataItem = (DataItem) op.item();
		final MessageDigest digest = checksumStrategy.digestSupplier.get();

		try {
			// Reset the digest
			digest.reset();

			// Allocate temp buffer
			ByteBuffer dst = ByteBuffer.allocate(64 * 1024);
			int bytesRead = 0;
			String checksum = null;

			while (true) {
				// Limit to remaining bytes if buffer capacity exceeds data item size
				if (bytesRead + dst.capacity() > dataItem.size()) {
					dst.limit((int) dataItem.size() - bytesRead);
				}

				// Read and update digest
				bytesRead += dataItem.read(dst);
				dst.flip();
				digest.update(dst);
				dst.clear();

				// Compute checksum and exit when entire data item read
				if (bytesRead == dataItem.size()) {
					byte[] checksumBytes = digest.digest();
					checksum = BASE64_ENCODER.encodeToString(checksumBytes);
					break;
				}
			}

			// Add checksum header
			httpHeaders.set(checksumStrategy.requestHeaderName, checksum);
		} catch (IOException e) {
			Loggers.ERR.info("Unable to compute checksum: {}", e.getMessage());
		} finally {
			// Always reset the data item
			dataItem.reset();
		}
	}

	@Override
	protected boolean integrityAdditionalPayloadPassRequired(final O op) {
		return checksumStrategy != null
						&& (op instanceof CompositeDataOperation
										|| !IntegrityMetadataCodec.ALGORITHM_SHA256.equals(checksumStrategy.configToken));
	}

	@Override
	protected void applyMetaDataHeaders(final HttpHeaders httpHeaders) {}

	@Override
	protected void applyMetaDataHeaders(final HttpHeaders httpHeaders, final O op) {
		super.applyMetaDataHeaders(httpHeaders, op);
		if (!integrityMetadataEnabled()
						|| !OpType.CREATE.equals(op.type())
						|| op.integrityMetadata() == null
						|| op instanceof PartialDataOperation) {
			return;
		}
		IntegrityMetadataCodec.httpHeaders(op.integrityMetadata()).forEach(httpHeaders::set);
	}

	final boolean integrityMetadataEnabledForResponse() {
		return integrityMetadataEnabled();
	}

	final void recordIntegrityReadResultFromResponse(final IntegrityVerificationResult result) {
		recordIntegrityReadResult(result);
	}

	final void recordTerminalResponseFailure(final IntegrityTerminalException failure) {
		recordTerminalFailure(failure);
	}

	/** Extension hook for transports which deliver a successful GET body outside HTTP. */
	protected boolean observesReadBodyOutOfBand(final O op) {
		return false;
	}

	final boolean observesIntegrityReadBodyOutOfBand(final O op) {
		return observesReadBodyOutOfBand(op);
	}

	protected final void finishOutOfBandIntegrityRead(
					final Channel channel, final O op, final java.nio.ByteBuffer body) {
		final IntegrityVerificationResult result = S3ResponseHandler.finishOutOfBandIntegrityRead(channel, body);
		if (result != null) {
			op.integrityVerificationResult(result);
			recordIntegrityReadResult(result);
			if (!result.verified()) {
				op.status(Operation.Status.RESP_FAIL_CORRUPT);
			}
		}
	}

	protected final void discardOutOfBandIntegrityRead(final Channel channel) {
		S3ResponseHandler.discardOutOfBandIntegrityRead(channel);
	}

	HttpRequest initMultipartUploadRequest(final O op, final String nodeAddr) {
		final var item = op.item();
		final var srcPath = op.srcPath();
		if (srcPath != null && !srcPath.isEmpty()) {
			throw new AssertionError("Multipart copy operation is not implemented yet");
		}
		final var uri = dataUriPath(item, srcPath, op.dstPath(), OpType.CREATE) + "?uploads";
		final var httpHeaders = new DefaultHttpHeaders();
		if (nodeAddr != null) {
			httpHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		}
		httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		if (checksumStrategy != null) {
			httpHeaders.set(S3Api.AMZ_CHECKSUM_ALGORITHM_HEADER, checksumStrategy.mpuInitAlgorithmToken);
		}
		final var httpMethod = HttpMethod.POST;
		final var httpRequest = (HttpRequest) new DefaultHttpRequest(HTTP_1_1, httpMethod, uri, httpHeaders);
		applyMetaDataHeaders(httpHeaders, op);
		applyDynamicHeaders(httpHeaders);
		applySharedHeaders(httpHeaders);
		applyAuthHeaders(httpHeaders, httpMethod, uri, op.credential());
		return httpRequest;
	}

	@SuppressWarnings("unchecked")
	HttpRequest partUploadRequest(
					final PartialDataOperation partialDataOp, final String nodeAddr) {
		final var item = (I) partialDataOp.item();
		final var srcPath = partialDataOp.srcPath();
		final var uri = dataUriPath(item, srcPath, partialDataOp.dstPath(), OpType.CREATE)
						+ "?partNumber="
						+ (partialDataOp.partNumber() + 1)
						+ "&uploadId="
						+ partialDataOp.parent().get(S3Api.KEY_UPLOAD_ID);
		final HttpHeaders httpHeaders = new DefaultHttpHeaders();
		if (nodeAddr != null) {
			httpHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		}
		final var httpMethod = HttpMethod.PUT;
		final HttpRequest httpRequest = new DefaultHttpRequest(HTTP_1_1, httpMethod, uri, httpHeaders);
		try {
			httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, ((DataItem) item).size());
		} catch (final IOException e) {
			Loggers.ERR.warn("Failed to get part size for {}: {}", item.name(), e.getMessage());
			partialDataOp.status(Operation.Status.FAIL_IO);
		}
		applyChecksum(httpHeaders, (O) partialDataOp);
		applyMetaDataHeaders(httpHeaders);
		applyDynamicHeaders(httpHeaders);
		applySharedHeaders(httpHeaders);
		applyAuthHeaders(httpHeaders, httpMethod, uri, partialDataOp.credential());
		return httpRequest;
	}

	@SuppressWarnings("unchecked")
	HttpRequest rangeReadRequest(
					final PartialDataOperation partialOp, final String nodeAddr) {
		final var parentOp = partialOp.parent();
		final var item = (I) parentOp.item();
		final var srcPath = partialOp.srcPath();
		final var uri = dataUriPath(item, srcPath, partialOp.dstPath(), OpType.READ);
		final HttpHeaders httpHeaders = new DefaultHttpHeaders();
		if (nodeAddr != null) {
			httpHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		}
		httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		// Compute byte range from part number and size threshold
		final long sizeThreshold = parentOp.sizeThreshold();
		final long rangeStart = (long) partialOp.partNumber() * sizeThreshold;
		long partSize;
		try {
			partSize = ((DataItem) partialOp.item()).size();
		} catch (final IOException e) {
			partialOp.status(Operation.Status.FAIL_IO);
			partSize = sizeThreshold;
		}
		final long rangeEnd = rangeStart + partSize - 1;
		httpHeaders.set(HttpHeaderNames.RANGE, "bytes=" + rangeStart + "-" + rangeEnd);
		final var httpMethod = readMetadataOnly ? HttpMethod.HEAD : HttpMethod.GET;
		final var httpRequest = (HttpRequest) new DefaultHttpRequest(
						HTTP_1_1, httpMethod, uri, httpHeaders);
		applyDynamicHeaders(httpHeaders);
		applySharedHeaders(httpHeaders);
		applyAuthHeaders(httpHeaders, httpMethod, uri, partialOp.credential());
		return httpRequest;
	}

	private static final ThreadLocal<StringBuilder> THREAD_LOCAL_STRB = ThreadLocal.withInitial(StringBuilder::new);

	FullHttpRequest completeMultipartUploadRequest(
					final CompositeDataOperation mpuTask, final String nodeAddr) {
		final var content = THREAD_LOCAL_STRB.get();
		content.setLength(0);
		content.append(S3Api.COMPLETE_MPU_HEADER);
		final List<PartialDataOperation> subTasks = mpuTask.subOperations();
		int nextPartNum;
		String nextEtag;
		for (final var subTask : subTasks) {
			nextPartNum = subTask.partNumber() + 1;
			nextEtag = mpuTask.get(Integer.toString(nextPartNum));
			content
							.append(S3Api.COMPLETE_MPU_PART_NUM_START)
							.append(nextPartNum)
							.append(S3Api.COMPLETE_MPU_PART_NUM_END)
							.append(nextEtag)
							.append(S3Api.COMPLETE_MPU_PART_ETAG_END);
			if (checksumStrategy != null) {
				final var partChecksum = mpuTask.get(
								S3Api.KEY_PART_CHECKSUM_PREFIX + nextPartNum);
				if (partChecksum != null) {
					final var xmlElem = checksumStrategy.completeMultipartXmlElementName;
					content.append("\n\t\t<").append(xmlElem).append('>')
									.append(partChecksum)
									.append("</").append(xmlElem).append('>');
				}
			}
			content.append(S3Api.COMPLETE_MPU_PART_CLOSE);
		}
		content.append(S3Api.COMPLETE_MPU_FOOTER);
		final var srcPath = mpuTask.srcPath();
		final var item = (I) mpuTask.item();
		final var uploadId = mpuTask.get(S3Api.KEY_UPLOAD_ID);
		final var uri = dataUriPath(item, srcPath, mpuTask.dstPath(), OpType.CREATE) + "?uploadId=" + uploadId;
		final HttpHeaders httpHeaders = new DefaultHttpHeaders();
		httpHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		final var httpMethod = HttpMethod.POST;
		final var contentBytes = content.toString().getBytes(UTF_8);
		final FullHttpRequest httpRequest = new DefaultFullHttpRequest(
						HTTP_1_1,
						httpMethod,
						uri,
						Unpooled.wrappedBuffer(contentBytes),
						httpHeaders,
						EmptyHttpHeaders.INSTANCE);
		httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, contentBytes.length);
		applyMetaDataHeaders(httpHeaders);
		applyDynamicHeaders(httpHeaders);
		applySharedHeaders(httpHeaders);
		applyAuthHeaders(httpHeaders, httpMethod, uri, mpuTask.credential());
		return httpRequest;
	}

	HttpRequest abortMultipartUploadRequest(
					final CompositeDataOperation mpuTask, final String nodeAddr) {
		final var item = (I) mpuTask.item();
		final var uploadId = mpuTask.get(S3Api.KEY_UPLOAD_ID);
		final var uri = dataUriPath(item, mpuTask.srcPath(), mpuTask.dstPath(), OpType.CREATE)
						+ "?uploadId=" + uploadId;
		final var httpHeaders = new DefaultHttpHeaders();
		if (nodeAddr != null) {
			httpHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		}
		httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		final var httpMethod = HttpMethod.DELETE;
		final var httpRequest = (HttpRequest) new DefaultHttpRequest(HTTP_1_1, httpMethod, uri, httpHeaders);
		applyDynamicHeaders(httpHeaders);
		applySharedHeaders(httpHeaders);
		applyAuthHeaders(httpHeaders, httpMethod, uri, mpuTask.credential());
		return httpRequest;
	}

	/**
	 * Whether a response to {@code op} names its item {@code key~versionId}: true when {@link
	 * #httpRequest} sends {@code op} through {@link #objectVersioningRequest} under the legacy
	 * convention, which reads the version from the item name. The response handler then replaces the
	 * trailing {@code ~versionId} with the returned one instead of appending a second suffix.
	 */
	final boolean namesLegacyVersion(final O op) {
		return versioning
						&& !taggingEnabled
						&& !integrityMetadataEnabled()
						&& op.requestedVersionId() == null
						&& !(op instanceof DeleteRequestOperation)
						&& !(op instanceof CompositeDataOperation)
						&& !(op instanceof PartialDataOperation)
						&& !(op instanceof ListOperation);
	}

	/**
	 * {@code name} with its trailing {@code ~versionId}, if any, replaced by {@code ~versionId}: the
	 * split {@link #objectVersioningRequest} makes, so a name never collects two suffixes.
	 */
	static String withLegacyVersion(final String name, final String versionId) {
		final int indexOfVersionStart = name.lastIndexOf('~');
		if (indexOfVersionStart == -1) {
			return name + '~' + versionId;
		}
		if (name.length() - indexOfVersionStart - 1 == versionId.length() && name.endsWith(versionId)) {
			// Already key~versionId, as after a versioned READ or STAT of the version the response names.
			return name;
		}
		return name.substring(0, indexOfVersionStart) + '~' + versionId;
	}

	HttpRequest objectVersioningRequest(final O op, final String nodeAddr) throws URISyntaxException {
		final var item = (I) op.item();
		if (integrityMetadataEnabled() || op.requestedVersionId() != null) {
			return objectVersioningRequest(op, item, op.requestedVersionId(), true, nodeAddr);
		}
		// The legacy key~version convention, only for ordinary mode. The item is never renamed: the
		// channel owns the operation while the request is built, so a completion (an idle timeout) or a
		// retry that reads the item meanwhile sees the name that selects the version. The paths are built
		// by dataUriPath from the whole name, and the ~versionId it ends with is taken off.
		final String name = item.name();
		final int indexOfVersionStart = name.lastIndexOf('~');
		return objectVersioningRequest(
						op, item, indexOfVersionStart == -1 ? null : name.substring(indexOfVersionStart + 1), false, nodeAddr);
	}

	/**
	 * The source a copy's first request carries, which {@link #keptCopySource} keeps: on the legacy
	 * versioned route, the {@link #dataUriPath} without the name's {@code ~versionId}, as {@link
	 * #objectVersioningRequest} names the object; on every other route, the HTTP drivers' source.
	 * Both the request builders and a completion before any request was built use it.
	 */
	@Override
	protected String copySource(final O op) {
		final String copySource = super.copySource(op);
		if (!namesLegacyVersion(op)) {
			return copySource;
		}
		final String name = op.item().name();
		final int indexOfVersionStart = name.lastIndexOf('~');
		return indexOfVersionStart == -1
						? copySource
						: withoutVersionSuffix(copySource, name.substring(indexOfVersionStart + 1));
	}

	/**
	 * {@code uriPath}, which {@link #dataUriPath} built from a {@code key~versionId} item name, without
	 * that trailing {@code ~versionId}: the path of the key, as {@code dataUriPath} gives it for an item
	 * named {@code key}. The base {@code dataUriPath} puts the name last, so does an override that only
	 * changes what comes before it. An override whose path does not end with the suffix has rewritten
	 * the name itself; the key's path can't be known without renaming the item, so the request is not
	 * built and the operation fails at once with {@code FAIL_UNKNOWN}, rather than acting on another
	 * object.
	 */
	static String withoutVersionSuffix(final String uriPath, final String versionId) {
		return uriPath.substring(0, versionSuffixStart(uriPath, versionId));
	}

	/**
	 * The index of the {@code ~} that starts {@code uriPath}'s trailing {@code ~versionId}, where {@link
	 * #withoutVersionSuffix} cuts it; it throws the same {@link RejectedRequestException} when there is
	 * none.
	 */
	private static int versionSuffixStart(final String uriPath, final String versionId) {
		final int keyEnd = uriPath.length() - versionId.length() - 1;
		if (keyEnd > 0 && uriPath.charAt(keyEnd) == '~' && uriPath.endsWith(versionId)) {
			return keyEnd;
		}
		throw new RejectedRequestException(
						"dataUriPath gave " + uriPath + ", which does not end with the item's ~" + versionId
										+ ": the versioned request can't select the version");
	}

	private HttpRequest objectVersioningRequest(
					final O op,
					final I item,
					final String versionId,
					final boolean explicitVersionCarrier,
					final String nodeAddr)
					throws URISyntaxException {
		final var srcPath = op.srcPath();
		final var opType = op.type();
		final HttpHeaders httpHeaders = new DefaultHttpHeaders();
		httpHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		final var httpMethod = dataHttpMethod(opType);
		final boolean legacyVersion = !explicitVersionCarrier && versionId != null;
		final String itemUri = dataUriPath(item, srcPath, op.dstPath(), op.type());
		final int keyEnd = legacyVersion ? versionSuffixStart(itemUri, versionId) : itemUri.length();
		// GetObject and HeadObject (a READ, also with read-metadata-only, and a STAT) select a version
		// with the versionId query; x-amz-version-id is a response header, which S3 ignores on a request.
		final boolean useVersionQuery = versionId != null
						&& (explicitVersionCarrier
										|| OpType.DELETE.equals(opType)
										|| OpType.READ.equals(opType)
										|| OpType.STAT.equals(opType));
		final String encodedVersionId = useVersionQuery ? encodeVersionId(versionId) : null;
		final String uri;
		if (!useVersionQuery) {
			uri = legacyVersion ? itemUri.substring(0, keyEnd) : itemUri;
		} else if (legacyVersion && encodedVersionId.length() == versionId.length()) {
			// itemUri is <the key's path>~<versionId>, and the version ID needs no encoding (encoding
			// lengthens what it changes), so the URI is itemUri with that ~ replaced by ?versionId=, built in
			// one copy. V4 signs it as sent, and so does V2 unless the ID is empty, which V2 signs as the
			// bare ?versionId (see appendV2CanonicalResource).
			uri = versionQueryUri(itemUri, keyEnd);
		} else {
			// V2 signs the versionId value decoded (see appendV2CanonicalResource); V4 signs it encoded.
			uri = (legacyVersion ? itemUri.substring(0, keyEnd) : itemUri) + VERSION_ID_QUERY + encodedVersionId;
		}
		final var httpRequest = (HttpRequest) new DefaultHttpRequest(
						HTTP_1_1, httpMethod, uri, httpHeaders);
		switch (opType) {
		case CREATE:
			if (srcPath == null || srcPath.isEmpty()) {
				if (item instanceof DataItem) {
					try {
						httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, ((DataItem) item).size());
					} catch (final IOException ignored) {
						// Unavailable size metadata must not replace the existing operation outcome.
					}
				} else {
					httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
				}
			} else {
				applyCopyHeaders(httpHeaders, keptCopySource(op));
				httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
			}
			break;
		case READ:
			httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
			if (op instanceof DataOperation) {
				applyRangesHeaders(httpHeaders, (DataOperation) op);
			}
			break;
		case UPDATE:
			// S3 has no in-place update of a version; the header is kept as it was (out of scope).
			if (!explicitVersionCarrier && versionId != null) {
				httpHeaders.set("x-amz-version-id", versionId);
			}
			final var dataOp = (DataOperation) op;
			httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, dataOp.markedRangesSize());
			applyRangesHeaders(httpHeaders, dataOp);
			break;
		case DELETE:
			httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
			break;
		case NOOP:
		case LIST:
		case STAT:
			// Preserve existing versioned-request behavior for types without content headers.
			break;
		}
		applyMetaDataHeaders(httpHeaders, op);
		applyDynamicHeaders(httpHeaders);
		applySharedHeaders(httpHeaders);
		applyAuthHeaders(httpHeaders, httpRequest.method(), uri, op.credential());
		return httpRequest;
	}

	private static final ThreadLocal<StringBuilder> THR_LOC_VERSION_URI = ThreadLocal.withInitial(StringBuilder::new);

	/**
	 * {@code itemUri}, which ends with {@code ~versionId} from {@code keyEnd}, with that {@code ~}
	 * replaced by {@code ?versionId=}: the same string as {@code itemUri.substring(0, keyEnd) +
	 * VERSION_ID_QUERY + versionId}, without the key's path copied out first.
	 */
	private static String versionQueryUri(final String itemUri, final int keyEnd) {
		final StringBuilder uri = THR_LOC_VERSION_URI.get();
		uri.setLength(0);
		return uri.append(itemUri, 0, keyEnd)
						.append(VERSION_ID_QUERY)
						.append(itemUri, keyEnd + 1, itemUri.length())
						.toString();
	}

	FullHttpRequest objectTaggingRequest(final O op, final String nodeAddr) {
		final var srcPath = op.srcPath();
		final var item = (I) op.item();
		final var uri = dataUriPath(item, srcPath, op.dstPath(), OpType.CREATE) + "?tagging";
		final HttpHeaders httpHeaders = new DefaultHttpHeaders();
		httpHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		final var opType = op.type();
		var httpRequest = (FullHttpRequest) null;
		switch (opType) {
		case READ:
			httpRequest = new DefaultFullHttpRequest(
							HTTP_1_1,
							GET,
							uri,
							Unpooled.EMPTY_BUFFER,
							httpHeaders,
							EmptyHttpHeaders.INSTANCE);
			break;
		case UPDATE:
			final var content = taggingContentInput.get();
			final var contentBytes = content.getBytes(UTF_8);
			httpRequest = new DefaultFullHttpRequest(
							HTTP_1_1,
							PUT,
							uri,
							Unpooled.wrappedBuffer(contentBytes),
							httpHeaders,
							EmptyHttpHeaders.INSTANCE);
			httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, contentBytes.length);
			final var contentMd5 = THREAD_LOCAL_MD5.get().digest(contentBytes);
			httpHeaders.set(HttpHeaderNames.CONTENT_MD5, BASE64_ENCODER.encodeToString(contentMd5));
			break;
		case DELETE:
			httpRequest = new DefaultFullHttpRequest(
							HTTP_1_1,
							DELETE,
							uri,
							Unpooled.EMPTY_BUFFER,
							httpHeaders,
							EmptyHttpHeaders.INSTANCE);
			break;
		default:
			throw new AssertionError(stepId + ": object tagging is not supported for " + opType);
		}
		applyMetaDataHeaders(httpHeaders);
		applyDynamicHeaders(httpHeaders);
		applySharedHeaders(httpHeaders);
		applyAuthHeaders(httpHeaders, httpRequest.method(), uri, op.credential());
		return httpRequest;
	}

	@Override
	protected boolean submit(final O op) throws IllegalStateException {
		if (isCompositeRead(op)) {
			final var compositeReadOp = (CompositeDataOperation) op;
			// Ensure sub-tasks exist. For recycled ops (result() copy), the subTasks
			// list is empty and pendingSubTasksCount==0 from the previous cycle.
			// Calling subOperations() regenerates them, resetting
			// pendingSubTasksCount to N so allSubOperationsDone() returns false.
			compositeReadOp.subOperations();
			if (compositeReadOp.allSubOperationsDone()) {
				if (!beginDispatch(op)) {
					return false;
				}
				// Finalization pass: all range-GETs ended. The parent is SUCC unless a range ran
				// out of retries, then propagate to the metrics/output pipeline. Duration will be
				// measured from the initial startRequest() to this finishResponse().
				final String rangeFailure = compositeReadOp.get("mpuFailure");
				op.status(rangeFailure == null ? Operation.Status.SUCC : Operation.Status.valueOf(rangeFailure));
				complete(null, op);
				return true;
			}
			// Initial pass: no HTTP request needed (range-GETs are stateless, unlike
			// MPU which requires an initiate call). Reset timing so recycled ops get
			// clean metrics, then complete immediately to trigger sub-operation
			// dispatch in handleCompleted().
			op.reset();
			if (concurrencyThrottle.tryAcquire()) {
				if (!beginDispatch(op)) {
					concurrencyThrottle.release();
					return false;
				}
				op.startRequest();
				op.finishRequest();
				concurrencyThrottle.release();
				op.status(Operation.Status.SUCC);
				op.startResponse();
				complete(null, op);
				return true;
			} else {
				return false;
			}
		}
		return super.submit(op);
	}

	@Override
	protected int submit(final List<O> ops, final int from, final int to)
					throws IllegalStateException {
		final var availablePermits = concurrencyThrottle.availablePermits();
		if (availablePermits == 0) {
			return 0;
		}
		final var dispatchLimit = from + Math.min(availablePermits, to - from);
		var submitted = 0;
		var i = from;
		while (i < dispatchLimit) {
			final var op = ops.get(i);
			if (isCompositeRead(op)) {
				if (!submit(op)) {
					break;
				}
				submitted++;
				i++;
			} else {
				final var segmentStart = i;
				while (i < dispatchLimit && !isCompositeRead(ops.get(i))) {
					i++;
				}
				final var segmentSubmitted = super.submit(ops, segmentStart, i);
				submitted += segmentSubmitted;
				if (segmentSubmitted < i - segmentStart) {
					break;
				}
			}
		}
		return submitted;
	}

	private boolean isCompositeRead(final O op) {
		return op instanceof CompositeDataOperation && OpType.READ.equals(op.type());
	}

	@Override
	public void complete(final Channel channel, final O op) {
		if (op instanceof DeleteRequestOperation deleteOperation) {
			if (deleteOperation.deleteResult() == null) {
				if (Operation.Status.SUCC.equals(op.status())) {
					deleteOperation.completeDelete(null);
				} else {
					deleteOperation.completeDelete(DeleteTransportResult.failure(
									deleteFailureStatus(op.status()),
									DELETE_FAILURE_MESSAGE));
				}
			}
			S3ResponseHandler.discardDeleteResponse(channel);
		}
		if (channel != null && op instanceof ListOperation) {
			try {
				S3ResponseHandler.discardListResponse(channel);
			} catch (final IOException e) {
				final var failure = new IntegrityTerminalException(
								IntegrityTerminalException.Category.CLEANUP,
								"failed to clean up aborted LIST response spool",
								e);
				recordTerminalFailure(failure);
				throw failure;
			}
		}
		if (channel != null && op instanceof CompositeDataOperation compositeOp
						&& OpType.CREATE.equals(op.type())) {
			final boolean abortRequested = compositeOp.get(S3Api.KEY_MPU_ABORT) != null;
			final boolean completing = !abortRequested && compositeOp.allSubOperationsDone();
			if (abortRequested) {
				final boolean abortSucceeded = Operation.Status.SUCC.equals(op.status());
				emitMultipartLifecycle(
								compositeOp,
								abortSucceeded ? "failed_aborted" : "failed_orphaned",
								true,
								abortSucceeded,
								abortSucceeded ? mpuFailure(compositeOp) : "abort failed: " + op.status());
				op.status(abortedUploadStatus(compositeOp));
			} else if (completing) {
				if (Operation.Status.SUCC.equals(op.status())) {
					emitMultipartLifecycle(compositeOp, "completed", false, null, null);
				} else if (compositeOp.get(S3Api.KEY_UPLOAD_ID) != null) {
					scheduleFailedCompletionAbort(compositeOp, op.status());
				}
			}
			// Pre-super: extract upload ID from channel (channel may be released by super)
			// and set failure status so super.complete() can close the channel if needed
			if (compositeOp.get(S3Api.KEY_MPU_ABORT) == null
							&& compositeOp.get(S3Api.KEY_UPLOAD_ID) == null
							&& !compositeOp.allSubOperationsDone()) {
				final var initUploadId = channel.attr(S3Api.KEY_ATTR_UPLOAD_ID).getAndSet(null);
				if (initUploadId == null) {
					// A failed initiation keeps its own status, so load-op-retry retries it when the
					// failure is transient; only an answer without an upload ID becomes not-found.
					if (Operation.Status.SUCC.equals(op.status())) {
						op.status(Operation.Status.RESP_FAIL_NOT_FOUND);
					}
				} else {
					compositeOp.put(S3Api.KEY_UPLOAD_ID, initUploadId);
				}
			}
			super.complete(channel, op);
			// Post-super: log with valid timing (finishResponse has set respTimeDone)
			final var uploadId = compositeOp.get(S3Api.KEY_UPLOAD_ID);
			if (compositeOp.get(S3Api.KEY_MPU_ABORT) != null) {
				// abort response — log ABORT phase
				Loggers.MULTIPART.info(
								"ABORT,{},{},{},{},{},{},{}",
								compositeOp.item().name(),
								uploadId != null ? uploadId : "",
								-1,
								compositeOp.reqTimeStart() / 1000,
								compositeOp.duration(),
								compositeOp.latency(),
								0);
			} else if (compositeOp.allSubOperationsDone()) {
				// all parts done — log PART rows for each sub-operation, then COMPLETE
				for (final var rawSubOp : compositeOp.subOperations()) {
					final var subOp = (PartialDataOperation<?>) rawSubOp;
					long partBytes = 0;
					try {
						partBytes = ((DataItem) subOp.item()).size();
					} catch (final IOException ignored) {
						// Unavailable size metadata must not replace the existing operation outcome.
					}
					Loggers.MULTIPART.info(
									"PART,{},{},{},{},{},{},{}",
									compositeOp.item().name(),
									uploadId,
									subOp.partNumber() + 1,
									subOp.reqTimeStart() / 1000,
									subOp.duration(),
									subOp.latency(),
									partBytes);
				}
				Loggers.MULTIPART.info(
								"COMPLETE,{},{},{},{},{},{},{}",
								compositeOp.item().name(),
								uploadId,
								-1,
								compositeOp.reqTimeStart() / 1000,
								compositeOp.duration(),
								compositeOp.latency(),
								0);
			} else if (uploadId != null) {
				// multipart upload has been initialized — log INIT phase
				Loggers.MULTIPART.info(
								"INIT,{},{},{},{},{},{},{}",
								compositeOp.item().name(),
								uploadId,
								-1,
								compositeOp.reqTimeStart() / 1000,
								compositeOp.duration(),
								compositeOp.latency(),
								0);
			}
		} else {
			if (channel == null) {
				settleUnsentMultipartRequest(op);
			}
			super.complete(channel, op);
		}
	}

	/**
	 * A multipart completion or abort that failed before it had a connection sent nothing, so its
	 * upload is still open: the completion's upload is aborted as after a failed response, and the
	 * abort's is recorded as orphaned (a kernel-route abort has recorded it already, and a second
	 * record is dropped). On the default route an operation the stop owns can get here too; the abort
	 * or the record is then cleanup only.
	 */
	private void settleUnsentMultipartRequest(final O op) {
		if (completionFailedWithoutAConnection(op)) {
			scheduleFailedCompletionAbort((CompositeDataOperation) op, op.status());
		} else if (abortsMultipartUpload(op) && failedUnsent(op.status())) {
			final var compositeOp = (CompositeDataOperation) op;
			try {
				emitMultipartLifecycle(compositeOp, "failed_orphaned", true, false,
								mpuFailure(compositeOp) + "; abort failed: " + op.status());
			} catch (final RuntimeException e) {
				LogUtil.exception(Level.DEBUG, e, "{}: failed to record an orphaned multipart upload", stepId);
			}
			// The upload ends with its part's failure, as after an answered abort.
			op.status(abortedUploadStatus(compositeOp));
		}
	}

	/**
	 * Whether {@code op} is a multipart upload's completion that failed before it had a connection: a
	 * failed lease or connect, on the default route, with endpoint selection or on a NIC set. A refused
	 * or timed-out connect is FAIL_IO; a lease that throws anything else is FAIL_UNKNOWN.
	 */
	static boolean completionFailedWithoutAConnection(final Operation<?> op) {
		return op instanceof CompositeDataOperation compositeOp
						&& OpType.CREATE.equals(op.type())
						&& failedUnsent(op.status())
						&& compositeOp.get(S3Api.KEY_MPU_ABORT) == null
						&& compositeOp.get(S3Api.KEY_UPLOAD_ID) != null
						&& compositeOp.allSubOperationsDone();
	}

	/**
	 * Whether a request that never had a connection failed: any failure but an interrupt, which is
	 * rethrown before the operation completes, so its operation stays with the stop.
	 */
	private static boolean failedUnsent(final Operation.Status status) {
		return status != null && !Operation.Status.SUCC.equals(status) && !Operation.Status.INTERRUPTED.equals(status)
						&& !Operation.Status.PENDING.equals(status) && !Operation.Status.ACTIVE.equals(status)
						&& !Operation.Status.OMIT.equals(status);
	}

	private static Operation.Status deleteFailureStatus(final Operation.Status status) {
		if (status == null) {
			return Operation.Status.FAIL_UNKNOWN;
		}
		return switch (status) {
		case FAIL_UNKNOWN,
						FAIL_IO,
						FAIL_TIMEOUT,
						RESP_FAIL_UNKNOWN,
						RESP_FAIL_CLIENT,
						RESP_FAIL_SVC,
						RESP_FAIL_NOT_FOUND,
						RESP_FAIL_AUTH,
						RESP_FAIL_CORRUPT,
						RESP_FAIL_SPACE ->
			status;
		default -> Operation.Status.FAIL_UNKNOWN;
		};
	}

	@Override
	public boolean supportsStandaloneDeleteRequests() {
		return true;
	}

	/** The multipart completion error codes this driver's step has logged. */
	S3ResponseHandler.CompletionErrorCodeLog completionErrorCodeLog() {
		return completionErrorCodeLog;
	}

	/** True only the first time it is called: whether this step should warn of a multipart response's overflow. */
	boolean claimMultipartResponseOverflowWarning() {
		return multipartResponseOverflowWarned.compareAndSet(false, true);
	}

	/**
	 * An abort after a failed completion, still pending: its upload, the completion's status and when
	 * the abort was scheduled ({@link System#nanoTime()}).
	 */
	record PendingAbort(CompositeDataOperation op, Operation.Status completionStatus, long scheduledAtNanos) {}

	void scheduleFailedCompletionAbort(
					final CompositeDataOperation op, final Operation.Status completionStatus) {
		final CompletableFuture<Void> cleanupTask = new CompletableFuture<>();
		// Tracked before the closing flag is read, and the close sets that flag before its snapshot:
		// a task the snapshot misses sees the flag and sends nothing.
		multipartCleanupTasks.put(cleanupTask, new PendingAbort(op, completionStatus, System.nanoTime()));
		if (multipartCleanupClosing) {
			try {
				recordAbortNotSent(op, completionStatus, "not sent: the driver is closing");
			} finally {
				multipartCleanupTasks.remove(cleanupTask);
				cleanupTask.complete(null);
			}
			return;
		}
		ServiceTaskExecutor.VT_EXECUTOR.submit(() -> {
			try {
				abortAfterFailedCompletion(op, completionStatus);
				cleanupTask.complete(null);
			} catch (final Throwable failure) {
				LogUtil.exception(Level.WARN, failure, "{}: the abort after a failed multipart completion failed", stepId);
				cleanupTask.completeExceptionally(failure);
			} finally {
				multipartCleanupTasks.remove(cleanupTask);
			}
		});
	}

	/** Records an upload whose abort was not sent, or not answered by the close, as orphaned. */
	private void recordAbortNotSent(
					final CompositeDataOperation op, final Operation.Status completionStatus, final String why) {
		// The record goes to this step's file from any thread, the closing one among them.
		try (final var ctx = CloseableThreadContext.put(KEY_STEP_ID, stepId)) {
			emitMultipartLifecycle(op, "failed_orphaned", true, false,
							"completion failed: " + completionStatus + "; abort " + why);
		} catch (final RuntimeException e) {
			LogUtil.exception(Level.DEBUG, e, "{}: failed to record an orphaned multipart upload", stepId);
		}
	}

	/**
	 * The stop waits here, after any subclass's stop work and before the I/O shuts down, for the aborts
	 * that follow failed multipart completions, each up to {@link #MULTIPART_CLEANUP_STOP_WAIT_MILLIS}
	 * after it was scheduled or after the stop began, whichever is earlier. A
	 * subclass that overrides this calls {@code super.beforeIoShutdown()} last: its own stop work can
	 * schedule such an abort.
	 */
	@Override
	protected void beforeIoShutdown() {
		super.beforeIoShutdown();
		awaitMultipartCleanupBeforeIoShutdown();
	}

	private void awaitMultipartCleanupBeforeIoShutdown() {
		final long stopStart = System.nanoTime();
		final long waitNanos = TimeUnit.MILLISECONDS.toNanos(multipartCleanupStopWaitMillis);
		final Set<CompletableFuture<Void>> awaited = new HashSet<>();
		var unanswered = 0;
		while (true) {
			// Waited on once each: a task completes its future before it leaves the map.
			final List<Map.Entry<CompletableFuture<Void>, PendingAbort>> pending = new ArrayList<>();
			for (final var entry : multipartCleanupTasks.entrySet()) {
				if (!entry.getKey().isDone() && awaited.add(entry.getKey())) {
					pending.add(entry);
				}
			}
			if (pending.isEmpty()) {
				break;
			}
			for (var i = 0; i < pending.size(); i++) {
				final var entry = pending.get(i);
				// From the schedule, so a worker's contexts stopping in turn do not add up their waits.
				final long deadline = Math.min(entry.getValue().scheduledAtNanos(), stopStart) + waitNanos;
				try {
					entry.getKey().get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
				} catch (final ExecutionException e) {
					// The failed task logged its failure.
				} catch (final TimeoutException e) {
					unanswered++;
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
					Loggers.ERR.warn("{}: interrupted waiting for {} multipart abort(s); the I/O shutdown ends them",
									stepId, unanswered + pending.size() - i);
					return;
				}
			}
		}
		if (unanswered > 0) {
			Loggers.ERR.warn("{}: {} multipart abort(s) unanswered {} ms after they were sent; the I/O shutdown ends them",
							stepId, unanswered, multipartCleanupStopWaitMillis);
		}
	}

	private long pendingCleanupCount() {
		return multipartCleanupTasks.keySet().stream().filter(task -> !task.isDone()).count();
	}

	/**
	 * The close's bound on the aborts still pending, a backstop after the stop's wait: an abort waits
	 * up to the net timeout for its connect and again for its answer, so twice the timeout plus a
	 * margin; with no net timeout (0 or less), a fixed cap.
	 */
	long multipartCleanupCloseBoundMillis() {
		return netTimeoutMilliSec < Integer.MAX_VALUE
						? 2L * netTimeoutMilliSec + MULTIPART_CLEANUP_CLOSE_MARGIN_MILLIS
						: multipartCleanupCloseCapMillis;
	}

	/** Test seam: the stop's wait in place of {@link #MULTIPART_CLEANUP_STOP_WAIT_MILLIS}. */
	final void multipartCleanupStopWaitMillis(final long millis) {
		multipartCleanupStopWaitMillis = millis;
	}

	/** Test seam: the close's cap in place of {@link #MULTIPART_CLEANUP_CLOSE_CAP_MILLIS}. */
	final void multipartCleanupCloseCapMillis(final long millis) {
		multipartCleanupCloseCapMillis = millis;
	}

	/** The aborts after failed completions not yet done. */
	final int pendingMultipartCleanupTasks() {
		return (int) pendingCleanupCount();
	}

	/** Test seam: runs in the close right after it marks the driver closing, before its snapshot. */
	void afterMultipartCleanupClosing() {}

	private void closeMultipartCleanup() {
		multipartCleanupClosing = true;
		afterMultipartCleanupClosing();
		final Map<CompletableFuture<Void>, PendingAbort> pending = new HashMap<>(multipartCleanupTasks);
		pending.keySet().removeIf(CompletableFuture::isDone);
		if (pending.isEmpty()) {
			return;
		}
		final long bound = multipartCleanupCloseBoundMillis();
		try {
			CompletableFuture.allOf(pending.keySet().toArray(CompletableFuture[]::new)).get(bound, TimeUnit.MILLISECONDS);
			return;
		} catch (final ExecutionException e) {
			// The failed task logged its failure; the others are done too.
			return;
		} catch (final TimeoutException e) {
			// Recorded below.
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		// Those still pending when the wait ended; a record a task writes later is dropped as a second one.
		final var orphaned = new ArrayList<PendingAbort>();
		pending.forEach((task, abort) -> {
			if (!task.isDone()) {
				orphaned.add(abort);
			}
		});
		for (final var abort : orphaned) {
			recordAbortNotSent(abort.op(), abort.completionStatus(), "pending at close");
		}
		if (!orphaned.isEmpty()) {
			Loggers.ERR.warn("{}: {} multipart abort(s) still pending at close after {} ms; their uploads are recorded orphaned",
							stepId, orphaned.size(), bound);
		}
	}

	void abortAfterFailedCompletion(
					final CompositeDataOperation op, final Operation.Status completionStatus) {
		FullHttpResponse response = null;
		try {
			final HttpRequest abortRequest = abortMultipartUploadRequest(op, helperAuthority);
			final FullHttpRequest fullAbortRequest = new DefaultFullHttpRequest(
							abortRequest.protocolVersion(),
							abortRequest.method(),
							abortRequest.uri(),
							Unpooled.EMPTY_BUFFER,
							abortRequest.headers(),
							EmptyHttpHeaders.INSTANCE);
			response = executeHttpRequest(fullAbortRequest);
			final boolean succeeded = response != null
							&& HttpStatusClass.SUCCESS.equals(response.status().codeClass());
			emitMultipartLifecycle(
							op,
							succeeded ? "failed_aborted" : "failed_orphaned",
							true,
							succeeded,
							"completion failed: " + completionStatus + (succeeded ? "" : "; abort failed"));
		} catch (final Exception e) {
			emitMultipartLifecycle(
							op,
							"failed_orphaned",
							true,
							false,
							"completion failed: " + completionStatus
											+ "; abort failed: " + e.getClass().getSimpleName());
		} finally {
			if (response != null) {
				response.release();
			}
		}
	}

	/** Whether {@code op} is a multipart upload's abort: its parent's request once a part ran out of retries. */
	protected static boolean abortsMultipartUpload(final Operation<?> op) {
		return op instanceof CompositeDataOperation compositeOp
						&& OpType.CREATE.equals(op.type())
						&& compositeOp.get(S3Api.KEY_MPU_ABORT) != null;
	}

	/**
	 * Sends a NIC-set driver's multipart abort over the kernel route, as the abort after a failed
	 * completion goes (D87): a connection of its own on the base event-loop group to {@code
	 * destination}, the address the step resolved for the first endpoint, started without blocking
	 * the dispatch task, so a dead NIC cannot leave the upload open. It takes no NIC and counts on
	 * no Path row. The caller holds the operation's concurrency permit, and its {@code
	 * releaseTransport} closes a connection with no Path and gives the permit back, so the abort
	 * completes as any request does.
	 *
	 * <p>Custody passes at the write, as for an attempt on a NIC: after a stop, or for a stale
	 * circulation, the connection closes, nothing is written and queue recovery owns the operation.
	 * {@link #closeKernelRoute} settles a connect still pending at the stop the same way. A connect
	 * that fails, or a {@code null} destination, fails the operation with {@code FAIL_IO} and
	 * records the upload as orphaned.
	 */
	protected final void submitOnKernelRoute(final O op, final DispatchToken<O> token,
					final InetSocketAddress destination, final int connectTimeoutMillis) {
		final var abort = new KernelRouteAbort(op, token);
		if (!kernelRouteAborts.register(abort)) {
			// Stopped: nothing starts, and queue recovery owns the operation.
			concurrencyThrottle.release();
			signalDispatchCapacityAvailable();
			return;
		}
		if (destination == null) {
			kernelRouteFailed(abort, new ConnectException("No address was resolved for " + storageNodeAddrs[0]));
			return;
		}
		final ChannelFuture connect;
		try {
			connect = connectUnpooled(destination, connectTimeoutMillis);
		} catch (final RuntimeException e) {
			kernelRouteFailed(abort, e);
			return;
		}
		abort.connect = connect;
		if (abort.settled()) {
			// A stop settled it, and gave its permit back, before it saw this connect.
			settleKernelRouteChannel(stepId, connect.channel(), () -> {});
		}
		connect.addListener((ChannelFutureListener) future -> {
			if (future.isSuccess()) {
				kernelRouteConnected(abort, future.channel());
			} else {
				kernelRouteFailed(abort, future.cause());
			}
		});
	}

	/**
	 * Settles every kernel-route abort whose connect is still pending, and starts no more: each
	 * connection closes, its permit comes back and, after a stop, its upload, which the unsent abort
	 * leaves open, is unresolved at once. NIC-set
	 * drivers call it when admission closes and before their I/O shuts down, so no permit waits on
	 * a connect timeout or on a listener an event loop that terminated would never run.
	 */
	protected final void closeKernelRoute() {
		for (final var abort : kernelRouteAborts.close()) {
			if (abort.settle()) {
				final var connect = abort.connect;
				// Settled is for good: nothing else gives this permit back.
				settleKernelRouteChannel(stepId, connect == null ? null : connect.channel(), this::giveBackKernelRoutePermit);
				settleRefusedDispatch(abort.op);
			}
		}
	}

	private void giveBackKernelRoutePermit() {
		concurrencyThrottle.release();
		signalDispatchCapacityAvailable();
	}

	/**
	 * Closes the channel of a kernel-route abort the stop settled, when it has one, and then runs
	 * {@code giveBack} whatever the close does: a failure to close is logged, never thrown.
	 */
	static void settleKernelRouteChannel(final String stepId, final Channel channel, final Runnable giveBack) {
		try {
			if (channel != null) {
				closeKernelRouteChannel(channel);
			}
		} catch (final RuntimeException e) {
			LogUtil.exception(Level.DEBUG, e, "{}: failed to close a pending multipart abort's connection", stepId);
		} finally {
			giveBack.run();
		}
	}

	/**
	 * Closes a kernel-route connect's channel. One whose creation or initialisation failed never
	 * registered with an event loop, so its {@code close()} would throw; it is closed forcibly, as
	 * Netty closes a channel whose registration failed. Nothing can run on it.
	 */
	static void closeKernelRouteChannel(final Channel channel) {
		try {
			final var unusedClose = channel.close();
		} catch (final IllegalStateException notRegistered) {
			channel.unsafe().closeForcibly();
		}
	}

	/** The kernel-route aborts whose connect has not settled. */
	protected final int kernelRouteAbortsPending() {
		return kernelRouteAborts.pending();
	}

	/** Runs on the connection's event loop. */
	private void kernelRouteConnected(final KernelRouteAbort abort, final Channel channel) {
		if (!abort.settle()) {
			// The stop settled it first.
			final var unusedClose = channel.close();
			return;
		}
		final var op = abort.op;
		if (!withDispatchAdmission(() -> claimCapturedDispatch(op, abort.token), op)) {
			final var unusedClose = channel.close();
			concurrencyThrottle.release();
			signalDispatchCapacityAvailable();
			return;
		}
		try {
			channel.attr(ATTR_KEY_RELEASED).set(Boolean.FALSE);
			channel.attr(ATTR_KEY_OPERATION).set(op);
			op.nodeAddr(helperAuthority);
			op.startRequest();
			sendRequest(channel, op);
		} catch (final Throwable thrown) {
			throwUncheckedIfInterrupted(thrown);
			LogUtil.exception(Level.DEBUG, thrown, "{}: failed to send a multipart abort", stepId);
			op.status(Operation.Status.FAIL_UNKNOWN);
			completeAfterKernelRoute(channel, op);
		}
	}

	private void kernelRouteFailed(final KernelRouteAbort abort, final Throwable cause) {
		if (!abort.settle()) {
			// The stop settled it first.
			return;
		}
		final var op = abort.op;
		final boolean claimed = withDispatchAdmission(() -> claimCapturedDispatch(op, abort.token), op);
		concurrencyThrottle.release();
		signalDispatchCapacityAvailable();
		if (!claimed) {
			LogUtil.exception(Level.DEBUG, cause, "{}: multipart abort connect ended after the stop", stepId);
			return;
		}
		op.nodeAddr(helperAuthority);
		op.status(Operation.Status.FAIL_IO);
		try {
			final var level = kernelRouteAbortWarned.compareAndSet(false, true) ? Level.WARN : Level.DEBUG;
			LogUtil.exception(level, cause, "{}: a multipart abort could not connect to {}; the upload stays open"
							+ " (further failures are logged at DEBUG)", stepId, storageNodeAddrs[0]);
			final var compositeOp = (CompositeDataOperation) op;
			emitMultipartLifecycle(compositeOp, "failed_orphaned", true, false,
							mpuFailure(compositeOp) + "; abort failed: " + Operation.Status.FAIL_IO);
		} catch (final RuntimeException e) {
			LogUtil.exception(Level.DEBUG, e, "{}: failed to record an orphaned multipart upload", stepId);
		} finally {
			completeAfterKernelRoute(null, op);
		}
	}

	private void completeAfterKernelRoute(final Channel channel, final O op) {
		try {
			complete(channel, op);
		} catch (final Throwable cause) {
			throwUncheckedIfInterrupted(cause);
			LogUtil.exception(Level.DEBUG, cause, "Load operation result publication failure");
		}
	}

	/** A kernel-route abort from its submit until its connect settles, by its listener or by the stop. */
	private final class KernelRouteAbort {

		final O op;
		final DispatchToken<O> token;
		private final AtomicBoolean settled = new AtomicBoolean();
		// Written after registration: a stop that finds it null leaves closing the channel to the submit.
		volatile ChannelFuture connect;

		KernelRouteAbort(final O op, final DispatchToken<O> token) {
			this.op = op;
			this.token = token;
		}

		boolean settled() {
			return settled.get();
		}

		/** Whether this call settled it: exactly one of the listener, a failure and the stop does. */
		boolean settle() {
			if (!settled.compareAndSet(false, true)) {
				return false;
			}
			kernelRouteAborts.remove(this);
			return true;
		}
	}

	/** The pending kernel-route aborts, closed for good at the stop. */
	static final class KernelRouteAborts<A> {

		private final ReentrantLock lock = new ReentrantLock();
		private final Set<A> pending = new HashSet<>();
		private boolean closed;

		boolean register(final A abort) {
			lock.lock();
			try {
				return !closed && pending.add(abort);
			} finally {
				lock.unlock();
			}
		}

		void remove(final A abort) {
			lock.lock();
			try {
				pending.remove(abort);
			} finally {
				lock.unlock();
			}
		}

		List<A> close() {
			lock.lock();
			try {
				closed = true;
				return List.copyOf(pending);
			} finally {
				lock.unlock();
			}
		}

		int pending() {
			lock.lock();
			try {
				return pending.size();
			} finally {
				lock.unlock();
			}
		}
	}

	private void emitMultipartLifecycle(
					final CompositeDataOperation op,
					final String state,
					final boolean abortAttempted,
					final Boolean abortSucceeded,
					final String error) {
		if (!integrityMetadataEnabled()) {
			return;
		}
		final String bucket = multipartBucket(op);
		final String key = multipartKey(
						dataUriPath((I) op.item(), op.srcPath(), op.dstPath(), op.type()), bucket);
		IntegrityCsvArtifacts.logMultipartLifecycleOnce(
						op,
						IntegrityCsvArtifacts.nodeIdentity(),
						stepId,
						driverType(),
						bucket,
						key,
						op.get(S3Api.KEY_UPLOAD_ID),
						state,
						abortAttempted,
						abortSucceeded,
						error);
	}

	private String multipartBucket(final CompositeDataOperation op) {
		String path = op.dstPath();
		if (path == null || path.isBlank()) {
			path = namespace;
		}
		if (path == null) {
			return "";
		}
		final String relative = path.startsWith(SLASH) ? path.substring(1) : path;
		final int slash = relative.indexOf(SLASH);
		return slash < 0 ? relative : relative.substring(0, slash);
	}

	/** The object key of a request URI, the same before and after the item's name is recorded. */
	static String multipartKey(final String objectUri, final String bucket) {
		final String key = objectUri.startsWith(SLASH) ? objectUri.substring(1) : objectUri;
		return !bucket.isEmpty() && key.startsWith(bucket + SLASH) ? key.substring(bucket.length() + 1) : key;
	}

	private static String mpuFailure(final CompositeDataOperation op) {
		final String failure = op.get("mpuFailure");
		return failure == null ? "multipart part failed" : "multipart part failed: " + failure;
	}

	@Override
	protected void appendHandlers(final Channel channel) {
		super.appendHandlers(channel);
		channel.pipeline().addLast(new S3ResponseHandler<>(
						this,
						verifyFlag,
						versioning,
						checksumStrategy == null ? null : checksumStrategy.responseChecksumHeaderName));
	}

	@Override
	protected void doClose() throws IOException {
		try {
			try (final var ctx = CloseableThreadContext.put(KEY_STEP_ID, stepId)) {
				// A listing the step stopped before its last page still gets its one summary.
				nonGeneratedKeys.keySet().forEach(listing -> logNonGeneratedKeys(nonGeneratedKeys.remove(listing)));
				closeMultipartCleanup();
			} finally {
				multipartCleanupTasks.clear();
				super.doClose();
			}
		} finally {
			// The closing thread's entries go now, even when a cleanup task or super.doClose() fails.
			// Other signing threads keep theirs until they end; SPT's cached functions are static, so
			// those entries keep no driver reachable.
			signingKeyCreateFunctionCache.remove();
			signingKeyCache.remove();
		}
	}

	@Override
	protected final void applyCopyHeaders(final HttpHeaders httpHeaders, final String srcPath)
					throws URISyntaxException {
		httpHeaders.set(S3Api.KEY_X_AMZ_COPY_SOURCE, srcPath);
	}

	private static final byte[] HEX_ARRAY = "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);
	private static final char[] URI_HEX_DIGITS = "0123456789ABCDEF".toCharArray();

	protected static String bytesToHex(byte[] bytes) {
		byte[] hexChars = new byte[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = HEX_ARRAY[v >>> 4];
			hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
		}
		return new String(hexChars, StandardCharsets.UTF_8);
	}

	@Override
	protected final void applyAuthHeaders(
					final HttpHeaders httpHeaders,
					final HttpMethod httpMethod,
					final String dstUriPath,
					final Credential credential) {
		final String uid;
		final String secret;
		if (credential != null) {
			uid = credential.getUid();
			secret = credential.getSecret();
		} else if (this.credential != null) {
			uid = this.credential.getUid();
			secret = this.credential.getSecret();
		} else {
			return;
		}
		if (uid == null || secret == null) {
			return;
		}

		try {
			if (authVersion == 2) {
				final var mac = MAC_BY_SECRET.get().computeIfAbsent(secret, GET_MAC_BY_SECRET);
				final var canonicalForm = getCanonical(httpHeaders, httpMethod, dstUriPath);
				final var sigData = mac.doFinal(canonicalForm.getBytes(UTF_8));
				httpHeaders.set(
								HttpHeaderNames.AUTHORIZATION,
								S3Api.AUTH_PREFIX + uid + ':' + BASE64_ENCODER.encodeToString(sigData));
			} else if (authVersion == 4) {
				// Preserve non-default ports while ensuring canonical host formatting
				final String originalHostHeader = httpHeaders.get(HttpHeaderNames.HOST);
				if (originalHostHeader != null) {
					final int colonIdx = originalHostHeader.indexOf(':');
					if (colonIdx > 0) {
						final String hostPart = originalHostHeader.substring(0, colonIdx);
						final String portPart = originalHostHeader.substring(colonIdx + 1);
						if ("80".equals(portPart) || "443".equals(portPart)) {
							httpHeaders.set(HttpHeaderNames.HOST, hostPart);
						} else {
							httpHeaders.set(HttpHeaderNames.HOST, originalHostHeader);
						}
					}
				}

				// Set signing timestamp using SigV4 header
				final Instant now = Instant.now();
				httpHeaders.remove(HttpHeaderNames.DATE);
				httpHeaders.set(S3Api.AMZ_DATE_HEADER, DateUtil.formatAmazon(now));

				// Set payload header
				final String contentLengthHeader = httpHeaders.get(HttpHeaderNames.CONTENT_LENGTH);
				final long contentLength = contentLengthHeader == null ? 0 : Long.parseLong(contentLengthHeader);
				if (contentLength > 0) {
					httpHeaders.set(S3Api.AMZ_PAYLOAD_HEADER, S3Api.AMZ_UNSIGNED_PAYLOAD);
				} else {
					httpHeaders.set(S3Api.AMZ_PAYLOAD_HEADER, S3Api.AMZ_EMPTY_BODY_SHA256);
				}

				final var datetime = httpHeaders.get(S3Api.AMZ_DATE_HEADER);
				final var date = datetime.substring(0, 8); // 8 chars as Pattern("yyyyMMdd") goes

				if (Loggers.MSG.isTraceEnabled()) {
					Loggers.MSG.trace(
									"SigV4 payload header present={}", httpHeaders.contains(S3Api.AMZ_PAYLOAD_HEADER));
				}
				final var signingKeyCreateFunction = signingKeyCreateFunctionCache.get().computeIfAbsent(
								secret, s -> new SigningKeyCreateFunctionImpl(s, awsRegion, sigV4ServiceName));
				// SPT's own function keeps its secret's key of the last date. A function an extension put in
				// the cache is memoized by date, as it always was.
				final byte[] signingKey = signingKeyCreateFunction instanceof SigningKeyCreateFunctionImpl builtIn
								? builtIn.apply(date)
								: signingKeyCache.get().computeIfAbsent(date, signingKeyCreateFunction);
				final Map<String, String> sortedHeaders = getNonCanonicalHeaders(httpHeaders);
				final var canonicalForm = getCanonicalV4(httpHeaders, sortedHeaders, httpMethod, dstUriPath);
				byte[] encodedhash = THREAD_LOCAL_SHA256.get().digest(
								canonicalForm.getBytes(StandardCharsets.UTF_8));
				String stringToSign = "AWS4-HMAC-SHA256\n" + datetime + "\n" + date
								+ "/" + awsRegion + "/" + sigV4ServiceName + "/aws4_request\n" + bytesToHex(encodedhash);
				if (Loggers.MSG.isTraceEnabled()) {
					Loggers.MSG.trace("SigV4 stringToSign={}", stringToSign);
				}
				final var sigData = HmacSHA256(stringToSign, signingKey);
				// 240 is an educated guess based on how v4 auth header looks generally
				StringBuilder sb = new StringBuilder(240);
				sb.append(S3Api.AUTH_V4_PREFIX)
								.append("Credential=")
								.append(uid)
								.append('/')
								.append(date)
								.append("/")
								.append(awsRegion)
								.append("/")
								.append(sigV4ServiceName).append("/aws4_request, ")
								.append("SignedHeaders=")
								.append(String.join(";", sortedHeaders.keySet()))
								.append(", Signature=")
								.append(bytesToHex(sigData));
				if (Loggers.MSG.isTraceEnabled()) {
					Loggers.MSG.trace("SigV4 signedHeaders={}", String.join(";", sortedHeaders.keySet()));
					Loggers.MSG.trace("SigV4 authorization header={}", sb);
				}
				httpHeaders.set(
								HttpHeaderNames.AUTHORIZATION, sb.toString());
			} else {
				throw new AssertionError("Not implemented yet");
			}
		} catch (NoSuchAlgorithmException | InvalidKeyException e) {
			Loggers.MSG.warn("Couldn't complete HmacSHA256 for auth v4. Either secret is incorrect " +
							"or used algorithm doesn't exist. \n{}", e.getMessage());
		}
	}

	/**
	 * SigV4 covers the canonical query, so V4 signs the URI with its configured URI arguments. V2 keeps
	 * signing the bare path: its string to sign names only sub-resources, and ordinary arguments are not
	 * among them.
	 */
	@Override
	protected String authUri(final String uriPath, final String uri) {
		return authVersion == 4 ? uri : uriPath;
	}

	protected Map<String, String> getNonCanonicalHeaders(final HttpHeaders httpHeaders) {
		String headerName;
		final Map<String, String> sortedHeaders = new TreeMap<>();
		if (sharedHeaders != null) {
			for (final var header : sharedHeaders) {
				headerName = header.getKey().toLowerCase(Locale.ROOT);
				if (headerName.startsWith(S3Api.PREFIX_KEY_X_AMZ)) {
					sortedHeaders.put(headerName, header.getValue());
				}
			}
		}
		for (final var header : httpHeaders) {
			headerName = header.getKey().toLowerCase(Locale.ROOT);
			if (headerName.startsWith(S3Api.PREFIX_KEY_X_AMZ)) {
				sortedHeaders.put(headerName, header.getValue());
			}
		}
		return sortedHeaders;
	}

	protected String getCanonicalV4(final HttpHeaders httpHeaders, final Map<String, String> sortedNonCanonicalHeaders,
					final HttpMethod httpMethod, final String dstUriPath) {
		final var buffCanonical = BUFF_CANONICAL.get();
		buffCanonical.setLength(0); // reset/clear
		buffCanonical.append(httpMethod.name());
		buffCanonical.append('\n');
		final int queryIndex = dstUriPath.indexOf("?");
		if (queryIndex != -1) {
			buffCanonical.append(dstUriPath, 0, queryIndex);
		} else {
			// No query string
			buffCanonical.append(dstUriPath);
		}
		buffCanonical.append('\n');
		if (queryIndex != -1) {
			final int queryStart = queryIndex + 1;
			if (isCanonicalSingleParameter(dstUriPath, queryStart)) {
				// Already canonical: appended without copying it out first.
				buffCanonical.append(dstUriPath, queryStart, dstUriPath.length());
			} else if (queryStart < dstUriPath.length()) {
				buffCanonical.append(canonicalizeQueryParameters(dstUriPath.substring(queryStart)));
			}
			// An empty query (the URI ends with ?) has an empty canonical form: nothing to append.
		}
		buffCanonical.append('\n');
		for (final var header : S3Api.HEADERS_CANONICAL_V4) {
			if (httpHeaders.contains(header)) {
				for (final var headerValue : httpHeaders.getAll(header)) {
					sortedNonCanonicalHeaders.put(header.toString(), headerValue);
				}
			} else if (sharedHeaders != null && sharedHeaders.contains(header)) {
				for (final var headerValue : sharedHeaders.getAll(header)) {
					sortedNonCanonicalHeaders.put(header.toString(), headerValue);
				}
			}
		}

		// x-amz-*
		if (sortedNonCanonicalHeaders.isEmpty()) {
			buffCanonical.append('\n');
		} else {
			for (final var headerEntry : sortedNonCanonicalHeaders.entrySet()) {
				final var headerValue = normalizeHeaderValue(headerEntry.getValue());
				buffCanonical
								.append(headerEntry.getKey())
								.append(':')
								.append(headerValue)
								.append('\n');
			}
			buffCanonical.append('\n');
		}

		if (!sortedNonCanonicalHeaders.isEmpty()) {
			final var headerIterator = sortedNonCanonicalHeaders.keySet().iterator();
			while (headerIterator.hasNext()) {
				buffCanonical.append(headerIterator.next());
				if (headerIterator.hasNext()) {
					buffCanonical.append(';');
				}
			}
		}
		buffCanonical.append('\n');

		// Don't sign the payload
		final String payloadHashHeader = httpHeaders.get(S3Api.AMZ_PAYLOAD_HEADER);
		if (payloadHashHeader != null && !payloadHashHeader.isEmpty()) {
			buffCanonical.append(payloadHashHeader);
		} else if (Long.parseLong(httpHeaders.get(HttpHeaderNames.CONTENT_LENGTH)) > 0) {
			buffCanonical.append(S3Api.AMZ_UNSIGNED_PAYLOAD);
		} else {
			buffCanonical.append(S3Api.AMZ_EMPTY_BODY_SHA256);
		}

		if (Loggers.MSG.isTraceEnabled()) {
			Loggers.MSG.trace("Canonical representation:\n{}", buffCanonical);
			Loggers.MSG.trace("SigV4 canonical request={} ", buffCanonical);
		}
		return buffCanonical.toString();
	}

	protected String getCanonical(
					final HttpHeaders httpHeaders, final HttpMethod httpMethod, final String dstUriPath) {
		final var buffCanonical = BUFF_CANONICAL.get();
		buffCanonical.setLength(0); // reset/clear
		buffCanonical.append(httpMethod.name());
		for (final var headerName : S3Api.HEADERS_CANONICAL) {
			if (httpHeaders.contains(headerName)) {
				for (final var headerValue : httpHeaders.getAll(headerName)) {
					buffCanonical.append('\n').append(headerValue);
				}
			} else if (sharedHeaders != null && sharedHeaders.contains(headerName)) {
				buffCanonical.append('\n').append(sharedHeaders.get(headerName));
			} else {
				buffCanonical.append('\n');
			}
		}
		// x-amz-*
		final Map<String, String> sortedHeaders = getNonCanonicalHeaders(httpHeaders);
		for (final var sortedHeader : sortedHeaders.entrySet()) {
			buffCanonical
							.append('\n')
							.append(sortedHeader.getKey())
							.append(':')
							.append(sortedHeader.getValue());
		}
		buffCanonical.append('\n');
		appendV2CanonicalResource(buffCanonical, dstUriPath);
		if (Loggers.MSG.isTraceEnabled()) {
			Loggers.MSG.trace("Canonical representation:\n{}", buffCanonical);
		}
		return buffCanonical.toString();
	}

	/** {@link S3Api#SIGV2_SUB_RESOURCES} indexed by name length, for a lookup with no substring. */
	private static final String[][] SIGV2_SUB_RESOURCES_BY_LENGTH = subResourcesByLength();

	private static String[][] subResourcesByLength() {
		int maxLength = 0;
		for (final var name : S3Api.SIGV2_SUB_RESOURCES) {
			maxLength = Math.max(maxLength, name.length());
		}
		final var byLength = new String[maxLength + 1][];
		for (int length = 0; length <= maxLength; length++) {
			final int len = length;
			byLength[length] = S3Api.SIGV2_SUB_RESOURCES.stream()
							.filter(name -> name.length() == len)
							.sorted()
							.toArray(String[]::new);
		}
		return byLength;
	}

	/**
	 * Appends the SigV2 canonical resource of a request URI: the path, then only the query's
	 * sub-resources ({@link S3Api#SIGV2_SUB_RESOURCES}), sorted by name and joined with {@code &}.
	 * The {@code ?} is left out when no sub-resource remains. An empty value signs as the bare name,
	 * so {@code ?versions=} signs as {@code ?versions}. A {@code versionId} value that holds a
	 * {@code %} is signed percent-decoded, as V2 requires (the URI carries it encoded), so a version
	 * ID that needs encoding signs as itself. Every other non-empty value is signed as sent: AWS,
	 * botocore and MinIO sign the decoded value and SeaweedFS the re-encoded one, which agree for
	 * the unreserved characters that upload IDs and part numbers are made of.
	 *
	 * <p>A URI with no query, or whose query is already canonical (see {@link #isCanonicalV2Query}),
	 * is appended whole, as before the fix: that is every multipart, tagging, delete and versioning
	 * request SPT builds, and every {@code versionId} request whose ID is non-empty and needs no
	 * encoding. This method stays small, so it inlines into {@code getCanonical}; only a listing, a
	 * {@code storage-net-http-uri-args} query, an unsorted query, or a {@code versionId} that is empty
	 * or encoded takes {@link #appendFilteredV2Resource}.
	 */
	static void appendV2CanonicalResource(final StringBuilder buff, final String uri) {
		final int queryStart = uri.indexOf('?');
		if (queryStart < 0 || isCanonicalV2Query(uri, queryStart)) {
			buff.append(uri);
		} else {
			appendFilteredV2Resource(buff, uri, queryStart);
		}
	}

	/**
	 * Whether the query starting at {@code queryStart} (the {@code ?}) is already its own canonical
	 * form, so that {@link #appendFilteredV2Resource} would append it unchanged: at least one
	 * parameter, every parameter a sub-resource with no empty value and no {@code versionId} value to
	 * decode, and the names in sorted order. One pass over the query, with no allocation.
	 */
	static boolean isCanonicalV2Query(final String uri, final int queryStart) {
		final int uriLength = uri.length();
		int paramStart = queryStart + 1;
		int prevNameStart = -1;
		int prevNameEnd = -1;
		while (paramStart < uriLength) {
			final int amp = uri.indexOf('&', paramStart);
			final int paramEnd = amp < 0 ? uriLength : amp;
			final int eq = uri.indexOf('=', paramStart, paramEnd);
			final int nameEnd = eq < 0 ? paramEnd : eq;
			if (eq == paramEnd - 1 // an empty value signs as the bare name
							|| !isV2SubResource(uri, paramStart, nameEnd) // also rejects an empty parameter
							|| (eq >= 0 && isEncodedVersionId(uri, paramStart, eq, paramEnd))
							|| (prevNameStart >= 0
											&& compareRegions(uri, prevNameStart, prevNameEnd, paramStart, nameEnd) > 0)) {
				return false;
			}
			if (amp < 0) {
				return true;
			}
			prevNameStart = paramStart;
			prevNameEnd = nameEnd;
			paramStart = amp + 1;
		}
		// No parameter, or a trailing '&': the filtered form drops what the URI holds.
		return false;
	}

	/**
	 * Appends the path, then only the query's sub-resources, sorted and with empty values bare. The
	 * usual sorted query is filtered in one pass with no allocation; an unsorted one takes the
	 * sorting path.
	 */
	static void appendFilteredV2Resource(final StringBuilder buff, final String uri, final int queryStart) {
		// Append the whole URI and cut it back: one array copy, not a per-character append.
		final int resourceEnd = buff.length() + queryStart;
		buff.append(uri);
		buff.setLength(resourceEnd);
		final int uriLength = uri.length();
		int prevNameStart = -1;
		int prevNameEnd = -1;
		int paramStart = queryStart + 1;
		while (paramStart < uriLength) {
			int paramEnd = uri.indexOf('&', paramStart);
			if (paramEnd < 0) {
				paramEnd = uriLength;
			}
			int nameEnd = uri.indexOf('=', paramStart, paramEnd);
			if (nameEnd < 0) {
				nameEnd = paramEnd;
			}
			if (isV2SubResource(uri, paramStart, nameEnd)) {
				if (prevNameStart >= 0 && compareRegions(uri, prevNameStart, prevNameEnd, paramStart, nameEnd) > 0) {
					buff.setLength(resourceEnd);
					appendSortedV2SubResources(buff, uri, queryStart);
					return;
				}
				appendV2SubResource(buff, uri, prevNameStart < 0, paramStart, nameEnd, paramEnd);
				prevNameStart = paramStart;
				prevNameEnd = nameEnd;
			}
			paramStart = paramEnd + 1;
		}
	}

	private static void appendSortedV2SubResources(final StringBuilder buff, final String uri, final int queryStart) {
		final int uriLength = uri.length();
		// Each sub-resource takes three slots: name start, name end, parameter end.
		var spans = new int[12];
		int count = 0;
		int paramStart = queryStart + 1;
		while (paramStart < uriLength) {
			int paramEnd = uri.indexOf('&', paramStart);
			if (paramEnd < 0) {
				paramEnd = uriLength;
			}
			int nameEnd = uri.indexOf('=', paramStart, paramEnd);
			if (nameEnd < 0) {
				nameEnd = paramEnd;
			}
			if (isV2SubResource(uri, paramStart, nameEnd)) {
				if (3 * count + 3 > spans.length) {
					spans = Arrays.copyOf(spans, spans.length * 2);
				}
				// Stable insertion sort by name: equal names keep their order in the query.
				int i = count;
				while (i > 0 && compareRegions(uri, spans[3 * (i - 1)], spans[3 * (i - 1) + 1], paramStart, nameEnd) > 0) {
					System.arraycopy(spans, 3 * (i - 1), spans, 3 * i, 3);
					i--;
				}
				spans[3 * i] = paramStart;
				spans[3 * i + 1] = nameEnd;
				spans[3 * i + 2] = paramEnd;
				count++;
			}
			paramStart = paramEnd + 1;
		}
		for (int i = 0; i < count; i++) {
			appendV2SubResource(buff, uri, i == 0, spans[3 * i], spans[3 * i + 1], spans[3 * i + 2]);
		}
	}

	private static void appendV2SubResource(
					final StringBuilder buff, final String uri, final boolean first,
					final int nameStart, final int nameEnd, final int paramEnd) {
		buff.append(first ? '?' : '&');
		// nameEnd is either the parameter end (no value) or the '=' before the value.
		if (nameEnd + 1 < paramEnd && isEncodedVersionId(uri, nameStart, nameEnd, paramEnd)) {
			buff.append(uri, nameStart, nameEnd + 1).append(decodeQueryComponent(uri.substring(nameEnd + 1, paramEnd)));
		} else {
			buff.append(uri, nameStart, nameEnd + 1 < paramEnd ? paramEnd : nameEnd);
		}
	}

	private static final String VERSION_ID = "versionId";

	/**
	 * Whether the parameter whose name ends at the {@code =} at {@code eq} is {@code versionId} with a
	 * value that holds a {@code %}: one that V2 signs decoded. Checks the length first, so another
	 * name costs one comparison.
	 */
	private static boolean isEncodedVersionId(final String uri, final int nameStart, final int eq, final int paramEnd) {
		return eq - nameStart == VERSION_ID.length()
						&& uri.startsWith(VERSION_ID, nameStart)
						&& uri.indexOf('%', eq + 1, paramEnd) >= 0;
	}

	private static boolean isV2SubResource(final String uri, final int nameStart, final int nameEnd) {
		final int length = nameEnd - nameStart;
		if (length <= 0 || length >= SIGV2_SUB_RESOURCES_BY_LENGTH.length) {
			return false;
		}
		for (final var name : SIGV2_SUB_RESOURCES_BY_LENGTH[length]) {
			if (uri.startsWith(name, nameStart)) {
				return true;
			}
		}
		return false;
	}

	/** Compares two regions of {@code s} the way {@link String#compareTo} compares strings. */
	private static int compareRegions(final String s, final int aStart, final int aEnd, final int bStart, final int bEnd) {
		final int aLength = aEnd - aStart;
		final int bLength = bEnd - bStart;
		final int common = Math.min(aLength, bLength);
		for (int i = 0; i < common; i++) {
			final int diff = s.charAt(aStart + i) - s.charAt(bStart + i);
			if (diff != 0) {
				return diff;
			}
		}
		return aLength - bLength;
	}

	@Override
	public String toString() {
		return String.format(super.toString(), "s3");
	}
}
