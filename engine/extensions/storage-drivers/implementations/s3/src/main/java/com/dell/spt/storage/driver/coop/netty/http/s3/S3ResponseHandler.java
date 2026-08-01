package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.dell.spt.base.integrity.IntegrityResponseObserver;
import com.dell.spt.base.integrity.IntegrityVerificationResult;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.item.op.list.ListOperation;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;

import com.dell.spt.storage.driver.coop.netty.http.HttpResponseHandlerBase;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import org.apache.logging.log4j.Level;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
Created by andrey on 25.11.16.
*/
public final class S3ResponseHandler<I extends Item, O extends Operation<I>>
				extends HttpResponseHandlerBase<I, O> {

	private static final AttributeKey<ByteBuf> CONTENT_ATTR_KEY = AttributeKey.newInstance(
					"content");
	private static final AttributeKey<ListResponseSpool> LIST_CONTENT_ATTR_KEY = AttributeKey.newInstance(
					"spt-list-content");
	private static final AttributeKey<IntegrityResponseObserver> INTEGRITY_OBSERVER_ATTR_KEY = AttributeKey.newInstance("spt-integrity-observer");
	private static final AttributeKey<Boolean> OUT_OF_BAND_READ_ATTR_KEY = AttributeKey.newInstance("spt-integrity-out-of-band-read");
	private static final int MIN_CONTENT_SIZE = 0x100;
	private static final int MAX_CONTENT_SIZE = 0x400;
	private static final AtomicInteger ACTIVE_LIST_SPOOLS = new AtomicInteger();
	private static final Pattern PATTERN_UPLOAD_ID = Pattern.compile(
					"<UploadId>([^<]+)</UploadId>", Pattern.MULTILINE);

	private static final class ListResponseSpool implements AutoCloseable {
		private final Path path;
		private OutputStream output;
		private long size;
		private boolean closed;

		private ListResponseSpool() throws IOException {
			path = Files.createTempFile("spt-list-response-", ".xml");
			try {
				output = Files.newOutputStream(path);
			} catch (final IOException e) {
				Files.deleteIfExists(path);
				throw e;
			}
			ACTIVE_LIST_SPOOLS.incrementAndGet();
		}

		private void write(final ByteBuf content) throws IOException {
			final int length = content.readableBytes();
			if (size + length > S3Api.MAX_LIST_RESPONSE_SPOOL_BYTES) {
				throw new IOException("LIST response exceeds the bounded 16 MiB parser spool");
			}
			content.readBytes(output, length);
			size += length;
		}

		private InputStream openInput() throws IOException {
			closeOutput();
			return Files.newInputStream(path);
		}

		private long size() {
			return size;
		}

		private void closeOutput() throws IOException {
			if (output != null) {
				output.close();
				output = null;
			}
		}

		public void close() throws IOException {
			if (closed) {
				return;
			}
			closed = true;
			IOException failure = null;
			try {
				closeOutput();
			} catch (final IOException e) {
				failure = e;
			}
			try {
				Files.deleteIfExists(path);
			} catch (final IOException e) {
				if (failure == null) {
					failure = e;
				} else {
					failure.addSuppressed(e);
				}
			} finally {
				ACTIVE_LIST_SPOOLS.decrementAndGet();
			}
			if (failure != null) {
				throw failure;
			}
		}
	}

	static int activeListSpoolCount() {
		return ACTIVE_LIST_SPOOLS.get();
	}

	private final S3StorageDriver<I, O> s3Driver;
	private final boolean versioningEnabled;
	private final String checksumHeader; // e.g. "x-amz-checksum-crc32c", or null if disabled

	public S3ResponseHandler(final S3StorageDriver<I, O> driver, final boolean verifyFlag,
					final boolean versioningEnabled, final String checksumHeader) {
		super(driver, verifyFlag);
		this.s3Driver = driver;
		this.versioningEnabled = versioningEnabled;
		this.checksumHeader = checksumHeader;
	}

	@Override
	protected final void handleResponseHeaders(final Channel channel, final O op, final HttpHeaders respHeaders) {
		if (op instanceof PartialDataOperation && OpType.CREATE.equals(op.type())) {
			// Capture part ETags for MPU write — needed for CompleteMultipartUpload XML body.
			// On error responses the server omits the ETag header; skip recording in that case
			// (the part will be retried, and the contextData map rejects null values).
			final String eTag = respHeaders.get(HttpHeaderNames.ETAG);
			if (eTag != null) {
				final PartialDataOperation subTask = (PartialDataOperation) op;
				final CompositeDataOperation mpuTask = subTask.parent();
				final int partNum = subTask.partNumber() + 1;
				mpuTask.put(Integer.toString(partNum), eTag);
				// Capture per-part checksum value if the server echoed one back
				if (checksumHeader != null) {
					final String checksumVal = respHeaders.get(checksumHeader);
					if (checksumVal != null) {
						mpuTask.put(S3Api.KEY_PART_CHECKSUM_PREFIX + partNum, checksumVal);
					}
				}
			}
		}
		final String versionId = respHeaders.get("x-amz-version-id");
		if (versionId != null) {
			op.returnedVersionId(versionId);
		}
		final String requestId = respHeaders.get("x-amz-request-id");
		if (requestId != null) {
			op.responseRequestId(requestId);
		}
		final boolean integrityEnabled = s3Driver != null && s3Driver.integrityMetadataEnabledForResponse();
		if (versioningEnabled && !integrityEnabled && versionId != null) {
			op.item().name(op.item().name() + "~" + versionId);
		}
		if (integrityEnabled
						&& channel != null
						&& OpType.READ.equals(op.type())
						&& op.status() == Operation.Status.SUCC
						&& op instanceof com.dell.spt.base.item.op.data.DataOperation
						&& !(op instanceof PartialDataOperation)
						&& !(op instanceof CompositeDataOperation)) {
			final boolean outOfBand = s3Driver.observesIntegrityReadBodyOutOfBand(op);
			channel.attr(OUT_OF_BAND_READ_ATTR_KEY).set(outOfBand);
			final Long contentLength = outOfBand
							? null
							: parseContentLength(respHeaders);
			channel.attr(INTEGRITY_OBSERVER_ATTR_KEY).set(
							new IntegrityResponseObserver(respHeaders, contentLength));
		}
	}

	@Override
	protected final void handleResponseContentChunk(final Channel channel, final O op, final ByteBuf contentChunk)
					throws IOException {
		if (channel != null) {
			final IntegrityResponseObserver observer = channel.attr(INTEGRITY_OBSERVER_ATTR_KEY).get();
			if (observer != null && contentChunk.isReadable()) {
				for (final var buffer : contentChunk.nioBuffers()) {
					observer.onBody(buffer);
				}
			}
		}
		if (op instanceof CompositeDataOperation) {
			handleInitMultipartUploadResponseContentChunk(channel, contentChunk);
		} else if (op instanceof ListOperation) {
			final ListOperation<?> listOp = (ListOperation<?>) op;
			markListDataResponseStart(listOp, contentChunk.readableBytes());
			if (integrityListDiscovery()) {
				bufferListContent(channel, listOp, contentChunk);
			} else {
				handleInitMultipartUploadResponseContentChunk(channel, contentChunk);
			}
		} else {
			super.handleResponseContentChunk(channel, op, contentChunk);
		}
	}

	static IntegrityVerificationResult finishOutOfBandIntegrityRead(
					final Channel channel, final java.nio.ByteBuffer body) {
		if (channel == null) {
			return null;
		}
		final IntegrityResponseObserver observer = channel.attr(INTEGRITY_OBSERVER_ATTR_KEY).getAndSet(null);
		if (observer == null) {
			return null;
		}
		observer.onBody(body);
		return observer.finish();
	}

	static void discardOutOfBandIntegrityRead(final Channel channel) {
		if (channel != null) {
			channel.attr(INTEGRITY_OBSERVER_ATTR_KEY).getAndSet(null);
		}
	}

	static void discardListResponse(final Channel channel) throws IOException {
		if (channel == null) {
			return;
		}
		final ListResponseSpool spool = channel.attr(LIST_CONTENT_ATTR_KEY).getAndSet(null);
		if (spool != null) {
			spool.close();
		}
	}

	private static Long parseContentLength(final HttpHeaders headers) {
		final String value = headers.get(HttpHeaderNames.CONTENT_LENGTH);
		if (value == null) {
			return null;
		}
		try {
			return Long.valueOf(value);
		} catch (final NumberFormatException ignored) {
			return null;
		}
	}

	private void markListDataResponseStart(final ListOperation<?> op, final int chunkSize) {
		if (chunkSize > 0 && op.respDataTimeStart() == 0) {
			try {
				op.startDataResponse();
			} catch (final IllegalStateException e) {
				LogUtil.exception(Level.DEBUG, e, "{}", op.toString());
			}
		}
	}

	private void bufferListContent(
					final Channel channel, final ListOperation<?> op, final ByteBuf contentChunk) {
		final Attribute<ListResponseSpool> contentAttr = channel.attr(LIST_CONTENT_ATTR_KEY);
		ListResponseSpool spool = contentAttr.get();
		try {
			if (spool == null) {
				spool = new ListResponseSpool();
				contentAttr.set(spool);
			}
			spool.write(contentChunk);
		} catch (final IOException e) {
			contentAttr.set(null);
			if (spool != null) {
				try {
					spool.close();
				} catch (final IOException cleanupFailure) {
					e.addSuppressed(cleanupFailure);
				}
			}
			throw recordListFailure(op, "failed to spool bounded LIST response", e);
		}
	}

	private void handleInitMultipartUploadResponseContentChunk(
					final Channel channel, final ByteBuf contentChunk) {
		// expect the XML data which is not large (up to 1KB)
		final Attribute<ByteBuf> contentAttr = channel.attr(CONTENT_ATTR_KEY);
		contentAttr.compareAndSet(null, Unpooled.buffer(MIN_CONTENT_SIZE));
		final ByteBuf content = contentAttr.get();
		try {
			content.writeBytes(contentChunk);
		} catch (final IndexOutOfBoundsException e) {
			LogUtil.exception(
							Level.WARN, e, "HTTP content input buffer overflow, expected no more than {} bytes",
							MAX_CONTENT_SIZE);
		}
	}

	@Override
	protected final void handleResponseContentFinish(final Channel channel, final O op) {
		final boolean outOfBand = Boolean.TRUE.equals(
						channel.attr(OUT_OF_BAND_READ_ATTR_KEY).getAndSet(null));
		final Attribute<IntegrityResponseObserver> observerAttr = channel.attr(INTEGRITY_OBSERVER_ATTR_KEY);
		final IntegrityResponseObserver observer = outOfBand && op.status() == Operation.Status.SUCC
						? observerAttr.get()
						: observerAttr.getAndSet(null);
		if (observer != null && op.status() == Operation.Status.SUCC && !outOfBand) {
			final var result = observer.finish();
			op.integrityVerificationResult(result);
			s3Driver.recordIntegrityReadResultFromResponse(result);
			if (!result.verified()) {
				op.status(Operation.Status.RESP_FAIL_CORRUPT);
			}
		}
		if (op instanceof ListOperation) {
			if (integrityListDiscovery()) {
				finishListResponse(channel, (ListOperation<?>) op);
			} else {
				finishOrdinaryListResponse(channel, (ListOperation<?>) op);
			}
		} else {
			final Attribute<ByteBuf> contentAttr = channel.attr(CONTENT_ATTR_KEY);
			final ByteBuf content = contentAttr.get();
			if (content != null && content.readableBytes() > 0 && op instanceof CompositeDataOperation) {
				final CompositeDataOperation mpuOp = (CompositeDataOperation) op;
				if (!mpuOp.allSubOperationsDone()) {
					// this is an MPU init response
					final String contentStr = content.toString(UTF_8);
					final Matcher m = PATTERN_UPLOAD_ID.matcher(contentStr);
					if (m.find()) {
						channel.attr(S3Api.KEY_ATTR_UPLOAD_ID).set(m.group(1));
					} else {
						Loggers.ERR.warn(
										"Upload id not found in the following response content:\n{}", contentStr);
					}
				}
				content.clear();
			}
		}
		super.handleResponseContentFinish(channel, op);
	}

	private void finishListResponse(final Channel channel, final ListOperation<?> op) {
		final ListResponseSpool spool = channel.attr(LIST_CONTENT_ATTR_KEY).getAndSet(null);
		if (op.status() != Operation.Status.SUCC) {
			if (spool != null) {
				try {
					spool.close();
				} catch (final IOException e) {
					throw recordListFailure(op, "failed to clean up LIST response spool", e);
				}
			}
			return;
		}
		if (spool == null || spool.size() == 0) {
			throw recordListFailure(op, "successful LIST response contained no XML document", null);
		}
		IntegrityTerminalException failure = null;
		try {
			parseIntegrityListResponse(spool.openInput(), op);
		} catch (final IntegrityTerminalException e) {
			failure = e;
		} catch (final IOException e) {
			failure = recordListFailure(op, "failed to open LIST response spool", e);
		}
		try {
			spool.close();
		} catch (final IOException e) {
			if (failure == null) {
				failure = recordListFailure(op, "failed to clean up LIST response spool", e);
			} else {
				failure.addSuppressed(e);
			}
		}
		if (failure != null) {
			throw failure;
		}
	}

	private boolean integrityListDiscovery() {
		return s3Driver != null && s3Driver.integrityMetadataEnabledForResponse();
	}

	private void finishOrdinaryListResponse(final Channel channel, final ListOperation<?> op) {
		final ByteBuf content = channel.attr(CONTENT_ATTR_KEY).get();
		if (content == null || !content.isReadable()) {
			return;
		}
		if (op.status() == Operation.Status.SUCC) {
			if (Loggers.MSG.isTraceEnabled()) {
				Loggers.MSG.trace("LIST raw response={}", content.toString(UTF_8));
			}
			try (final var contentStream = new ByteBufInputStream(content.duplicate())) {
				parseListDocument(contentStream, op);
			} catch (final Exception e) {
				LogUtil.exception(Level.WARN, e, "Failed to parse LIST response");
			}
		}
		content.clear();
	}

	private void parseIntegrityListResponse(final InputStream contentStream, final ListOperation<?> op) {
		try (contentStream) {
			parseListDocument(contentStream, op);
		} catch (final Exception e) {
			throw recordListFailure(op, "failed to parse LIST response", e);
		}
	}

	private void parseListDocument(final InputStream contentStream, final ListOperation<?> op)
					throws Exception {
		final var options = op.options();
		final var handler = new ListObjectsXmlHandler(
						op, options.includeVersions(), options.fetchMetadata());
		S3XmlParser.parse(contentStream, handler);
		final var driverId = driver != null ? driver.toString() : "s3";
		Loggers.MSG.trace(
						"{}: parsed LIST page objects={} bytes={} truncated={} token={}",
						driverId,
						op.objectsListed(),
						op.bytesListed(),
						op.truncated(),
						op.continuationToken());
	}

	private IntegrityTerminalException recordListFailure(
					final ListOperation<?> op, final String message, final Throwable cause) {
		op.status(Operation.Status.RESP_FAIL_CLIENT);
		final var failure = new IntegrityTerminalException(
						IntegrityTerminalException.Category.INPUT, message, cause);
		if (s3Driver != null) {
			s3Driver.recordTerminalResponseFailure(failure);
		}
		return failure;
	}
}
