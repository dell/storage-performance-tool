package com.dell.spt.storage.driver.coop.netty.http;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.dell.spt.base.item.DataItem.rangeCount;
import static com.dell.spt.base.item.DataItem.rangeOffset;
import static com.dell.spt.base.item.op.Operation.SLASH;
import static com.github.akurilov.commons.io.el.ExpressionInput.ASYNC_MARKER;
import static com.github.akurilov.commons.io.el.ExpressionInput.INIT_MARKER;
import static com.github.akurilov.commons.io.el.ExpressionInput.SYNC_MARKER;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.config.el.CompositeExpressionInputBuilder;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.InvalidTextValueException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemNamePaths;
import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.TokenItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.storage.driver.coop.netty.NettyStorageDriverBase;
import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.confuse.Config;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.TooLongHttpContentException;
import io.netty.handler.stream.ChunkedWriteHandler;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URISyntaxException;
import java.nio.channels.ClosedChannelException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;

/**
* Created by kurila on 29.07.16. Netty-based concurrent HTTP client executing the submitted load
* operations.
*/
public abstract class HttpStorageDriverBase<I extends Item, O extends Operation<I>>
				extends NettyStorageDriverBase<I, O> implements HttpStorageDriver<I, O> {

	private static final String CLS_NAME = HttpStorageDriverBase.class.getSimpleName();
	private static final String DELETE_REQUEST_TIMING_HANDLER = "deleteRequestTiming";

	private static Input<String> expressionInput(final String expression) {
		return CompositeExpressionInputBuilder.newInstance().expression(expression).build();
	}

	private final Map<String, Input<String>> headerNameInputs = new ConcurrentHashMap<>();
	private final Map<String, Input<String>> headerValueInputs = new ConcurrentHashMap<>();
	private final AtomicBoolean requestBuildFailureLogged = new AtomicBoolean(false);
	protected final HttpHeaders sharedHeaders = new DefaultHttpHeaders();
	protected final Map<String, String> dynamicHeaders = new HashMap<>();
	private final Input<String> uriQueryInput;
	protected final ChannelFutureListener httpReqSentCallback = this::sendHttpRequestComplete;
	protected final boolean readMetadataOnly;
	protected final int maxChunkSize;

	/**
	 * A request header's value. A list or map, or no value, which a defaults document can give, is
	 * refused with the header's name; a driver that sends no such headers ignores them, so the
	 * configuration keeps them. Any other value is cast as before.
	 */
	private static String headerText(final String name, final Object value) {
		final var path = ConfigUtil.HEADER_ARG_PATH_PREFIX + name;
		if (value == null) {
			throw new InvalidTextValueException(path, path + ": a request header needs a value");
		}
		InvalidTextValueException.refuseListOrMap(path, value);
		return (String) value;
	}

	/**
	 * A URI argument's value. A list or map is refused with the argument's name, as for a header; no
	 * value is kept and sent as {@code null}, as before.
	 */
	private static String uriArgTextOrNull(final String name, final Object value) {
		InvalidTextValueException.refuseListOrMap("storage-net-http-uri-args-" + name, value);
		return (String) value;
	}

	protected HttpStorageDriverBase(
					final String testStepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize)
					throws IllegalConfigurationException, InterruptedException {
		super(testStepId, itemDataInput, storageConfig, verifyFlag, batchSize);
		final var httpConfig = storageConfig.configVal("net-http");
		final var headersMap = httpConfig.<Object> mapVal("headers");
		for (final var header : headersMap.entrySet()) {
			final var headerKey = header.getKey();
			final var headerValue = headerText(headerKey, header.getValue());
			if (headerKey.contains(ASYNC_MARKER)
							|| headerKey.contains(SYNC_MARKER)
							|| headerKey.contains(INIT_MARKER)
							|| headerValue.contains(ASYNC_MARKER)
							|| headerValue.contains(SYNC_MARKER)
							|| headerValue.contains(INIT_MARKER)) {
				dynamicHeaders.put(headerKey, headerValue);
			} else {
				sharedHeaders.add(headerKey, headerValue);
			}
		}
		this.readMetadataOnly = httpConfig.boolVal("read-metadata-only");
		if (readMetadataOnly) {
			Loggers.MSG.info("Reading metadata only (HEAD requests)");
		}
		this.maxChunkSize = httpConfig.intVal("max-chunk-size");
		final var uriArgs = httpConfig.<Object> mapVal("uri-args");
		final var uriQueryExpr = uriArgs.entrySet().stream()
						.map(entry -> entry.getKey() + '=' + uriArgTextOrNull(entry.getKey(), entry.getValue()))
						.collect(Collectors.joining("&"));
		if (uriQueryExpr.length() > 0) {
			uriQueryInput = expressionInput('?' + uriQueryExpr);
		} else {
			uriQueryInput = new ConstantValueInputImpl("");
		}
	}

	/**
	 * The largest response body a control request takes by default: 16 MiB, the metadata-mode LIST
	 * spool cap. A listing page of 1000 keys is about 6.6 MiB at worst. A {@code HEAD} response's
	 * {@code Content-Length} is never checked against it.
	 */
	protected static final int CONTROL_RESPONSE_MAX_CONTENT_LENGTH = 16 * 1024 * 1024;

	protected FullHttpResponse executeHttpRequest(final FullHttpRequest request)
					throws InterruptedException, ConnectException {
		return executeHttpRequest(request, true);
	}

	/** Executes a synchronous control request with caller-selected timeout log severity. */
	protected FullHttpResponse executeHttpRequest(
					final FullHttpRequest request, final boolean warnOnTimeout)
					throws InterruptedException, ConnectException {
		return executeHttpRequest(request, warnOnTimeout, CONTROL_RESPONSE_MAX_CONTENT_LENGTH);
	}

	/**
	 * Executes a synchronous control request whose response body may be up to {@code
	 * maxContentLength} bytes, for a caller whose answers can pass {@link
	 * #CONTROL_RESPONSE_MAX_CONTENT_LENGTH}. A larger body is refused with one warning and the result
	 * is null, as for any request left without a response.
	 */
	protected FullHttpResponse executeHttpRequest(
					final FullHttpRequest request, final boolean warnOnTimeout, final int maxContentLength)
					throws InterruptedException, ConnectException {
		ThreadContext.put(KEY_STEP_ID, stepId);
		ThreadContext.put(KEY_CLASS_NAME, CLS_NAME);
		final FullHttpResponse resp;
		final var channel = getUnpooledConnection(storageNodeAddrs[0], storageNodePort);
		// The response, or null once the channel goes inactive without one: a closed channel, the
		// driver's I/O shutdown among its causes, ends the wait at once instead of after the timeout.
		final var control = new ControlResponseHandler(request.method(), maxContentLength);
		final var fullResp = control.response();
		try {
			final var pipeline = channel.pipeline();
			Loggers.MSG.debug(
							"{}: execute the HTTP request using the channel {} w/ pipeline: {}",
							stepId,
							channel.hashCode(),
							pipeline);
			pipeline.removeLast(); // remove the API specific handler
			pipeline.addLast(control.aggregator());
			pipeline.addLast(control);
			channel.writeAndFlush(request).sync();
			resp = awaitResponse(control, request, warnOnTimeout);
		} catch (final NoSuchElementException e) {
			throw new ConnectException("Channel pipeline is empty: connectivity related failure");
		} catch (final InterruptedException e) {
			releaseUnclaimedResponse(fullResp);
			throwUnchecked(e);
			return null;
		} catch (final Exception e) {
			releaseUnclaimedResponse(fullResp);
			if (e instanceof ClosedChannelException) {
				throw new ConnectException("Connection is closed: " + e.toString());
			} else {
				throw new ConnectException("Connection failure: " + e.toString());
			}
		} finally {
			// Teardown is asynchronous; this synchronous request has already settled.
			final var unusedClose = channel.close();
		}
		return resp;
	}

	/**
	 * Takes one control request's response: completes {@link #response()} with it, or with null once
	 * the channel goes inactive without one. Its {@link #aggregator()} refuses a body over the cap and
	 * marks the response {@link #oversized()}, so the waiter can tell that from a plain close.
	 */
	static final class ControlResponseHandler extends SimpleChannelInboundHandler<HttpObject> {

		private final CompletableFuture<FullHttpResponse> response = new CompletableFuture<>();
		private final HttpObjectAggregator aggregator;
		private final int maxContentLength;
		private volatile boolean oversized;

		ControlResponseHandler(final HttpMethod method, final int maxContentLength) {
			this.maxContentLength = maxContentLength;
			this.aggregator = new ControlResponseAggregator(HttpMethod.HEAD.equals(method), maxContentLength, this);
		}

		CompletableFuture<FullHttpResponse> response() {
			return response;
		}

		/** The aggregator to place before this handler. */
		HttpObjectAggregator aggregator() {
			return aggregator;
		}

		int maxContentLength() {
			return maxContentLength;
		}

		/** Whether the response was refused for a body over the cap. */
		boolean oversized() {
			return oversized;
		}

		@Override
		protected void channelRead0(final ChannelHandlerContext ctx, final HttpObject msg) {
			if (msg instanceof FullHttpResponse full && !response.complete(full.retain())) {
				full.release();
			}
		}

		@Override
		public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
			response.complete(null);
			super.channelInactive(ctx);
		}

		@Override
		public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
			if (oversized && cause instanceof TooLongHttpContentException) {
				// The aggregator has closed the channel; the waiter logs the refusal once.
				response.complete(null);
			} else {
				super.exceptionCaught(ctx, cause);
			}
		}
	}

	/**
	 * A {@code HEAD} response has no body, so its {@code Content-Length} header, which states the
	 * object's size, is not checked against {@code maxContentLength}: an object larger than the cap
	 * would otherwise fail the request. Responses to other methods keep the limit.
	 */
	private static final class ControlResponseAggregator extends HttpObjectAggregator {

		private final boolean head;
		private final ControlResponseHandler owner;

		ControlResponseAggregator(
						final boolean head, final int maxContentLength, final ControlResponseHandler owner) {
			super(maxContentLength);
			this.head = head;
			this.owner = owner;
		}

		@Override
		protected boolean isContentLengthInvalid(final HttpMessage start, final int maxContentLength) {
			return !head && super.isContentLengthInvalid(start, maxContentLength);
		}

		@Override
		protected void handleOversizedMessage(final ChannelHandlerContext ctx, final HttpMessage oversized)
						throws Exception {
			// Marked before the channel closes, so the waiter woken by the close sees it.
			owner.oversized = true;
			super.handleOversizedMessage(ctx, oversized);
		}
	}

	/**
	 * Settles a control request's response that no caller will take: a response already read is
	 * released, and the handler releases any read after this.
	 */
	static void releaseUnclaimedResponse(final CompletableFuture<FullHttpResponse> fullResp) {
		if (!fullResp.complete(null)) {
			final var resp = fullResp.getNow(null);
			if (resp != null) {
				resp.release();
			}
		}
	}

	FullHttpResponse awaitResponse(
					final ControlResponseHandler control,
					final FullHttpRequest request,
					final boolean warnOnTimeout)
					throws InterruptedException, ExecutionException {
		final var fullResp = control.response();
		FullHttpResponse resp;
		try {
			resp = fullResp.get(netTimeoutMilliSec, TimeUnit.MILLISECONDS);
			if (resp == null) {
				if (control.oversized()) {
					Loggers.MSG.warn(
									"{}: Response body over the {}-byte cap for control requests, refused: {}",
									stepId,
									control.maxContentLength(),
									timeoutRequestDescription(request));
				} else {
					logUnanswered(warnOnTimeout, "Connection closed before the response", request);
				}
			}
		} catch (final TimeoutException e) {
			// A response that arrived meanwhile is kept; a later one is released by the handler.
			resp = fullResp.complete(null) ? null : fullResp.getNow(null);
			if (resp == null) {
				logUnanswered(warnOnTimeout, "Response timeout", request);
			}
		}
		return resp;
	}

	private void logUnanswered(final boolean warn, final String what, final FullHttpRequest request) {
		if (warn) {
			Loggers.MSG.warn("{}: {}: {}", stepId, what, timeoutRequestDescription(request));
		} else {
			Loggers.MSG.debug("{}: {}: {}", stepId, what, timeoutRequestDescription(request));
		}
	}

	static String timeoutRequestDescription(final FullHttpRequest request) {
		return "method=" + request.method() + ", uri=" + request.uri();
	}

	@Override
	protected void appendHandlers(final Channel channel) {
		super.appendHandlers(channel);
		channel
						.pipeline()
						.addFirst(DELETE_REQUEST_TIMING_HANDLER, new DeleteRequestTimingHandler())
						.addLast(new HttpClientCodec(REQ_LINE_LEN, HEADERS_LEN, maxChunkSize, true))
						.addLast(new ChunkedWriteHandler());
	}

	private static final class DeleteRequestTimingHandler extends ChannelDuplexHandler {
		@Override
		public void channelRead(
						final ChannelHandlerContext ctx, final Object msg) throws Exception {
			if (msg instanceof ByteBuf buffer && buffer.isReadable()) {
				final Operation<?> op = ctx.channel().attr(ATTR_KEY_OPERATION).get();
				if (op instanceof DeleteRequestOperation deleteOperation) {
					deleteOperation.markResponseFirstByteReceived();
				}
			}
			ctx.fireChannelRead(msg);
		}

		@Override
		public void write(
						final ChannelHandlerContext ctx,
						final Object msg,
						final ChannelPromise promise) throws Exception {
			if (msg instanceof ByteBuf buffer && buffer.isReadable()) {
				final Operation<?> op = ctx.channel().attr(ATTR_KEY_OPERATION).get();
				if (op instanceof DeleteRequestOperation deleteOperation) {
					deleteOperation.markRequestFirstByteSent();
				}
			}
			// The caller owns this exact promise and observes completion downstream.
			final var unusedWrite = ctx.write(msg, promise);
		}
	}

	protected HttpRequest httpRequest(final O op, final String nodeAddr) throws URISyntaxException {
		final var item = op.item();
		final var opType = op.type();
		final var srcPath = op.srcPath();
		final HttpMethod httpMethod;
		final String uriPath;
		if (item instanceof DataItem) {
			httpMethod = dataHttpMethod(opType);
			uriPath = dataUriPath(item, srcPath, op.dstPath(), opType);
		} else if (item instanceof TokenItem) {
			httpMethod = tokenHttpMethod(opType);
			uriPath = tokenUriPath(item, srcPath, op.dstPath(), opType);
		} else if (item instanceof PathItem) {
			httpMethod = pathHttpMethod(opType);
			uriPath = pathUriPath(item, srcPath, op.dstPath(), opType);
		} else {
			throw new AssertionError("Unsupported item class: " + item.getClass().getName());
		}
		final var uriQuery = uriQuery();
		final var uri = uriQuery == null || uriQuery.isEmpty() ? uriPath : uriPath + uriQuery;
		final var httpHeaders = (HttpHeaders) new DefaultHttpHeaders();
		if (nodeAddr != null) {
			httpHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		}
		final var httpRequest = (HttpRequest) new DefaultHttpRequest(HTTP_1_1, httpMethod, uri, httpHeaders);
		switch (opType) {
		case CREATE:
			if (srcPath == null || srcPath.isEmpty()) {
				if (item instanceof DataItem) {
					try {
						httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, ((DataItem) item).size());
					} catch (final IOException ignored) {
						// Leave length unspecified when the item cannot provide size metadata.
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
			final var dataOp = (DataOperation) op;
			httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, dataOp.markedRangesSize());
			applyRangesHeaders(httpHeaders, dataOp);
			break;
		case DELETE:
			httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
			break;
		case STAT:
			httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
			break;
		case NOOP:
		case LIST:
			// These operation types do not add content headers here.
			break;
		}
		applyChecksum(httpHeaders, op);
		applyMetaDataHeaders(httpHeaders, op);
		applyDynamicHeaders(httpHeaders);
		applySharedHeaders(httpHeaders);
		applyAuthHeaders(httpHeaders, httpMethod, authUri(uriPath, uri), op.credential());
		return httpRequest;
	}

	/**
	 * Selects the URI that {@link #httpRequest} signs. The default signs the path without the
	 * configured URI arguments, as every driver has always done; a driver whose signature covers the
	 * query returns {@code uri}. Without URI arguments both are the same string.
	 *
	 * @param uriPath the request path without the URI arguments
	 * @param uri the request URI as sent: {@code uriPath} followed by the URI arguments, if any
	 * @return the URI passed to {@link #applyAuthHeaders}
	 */
	protected String authUri(final String uriPath, final String uri) {
		return uriPath;
	}

	protected HttpMethod dataHttpMethod(final OpType opType) {
		switch (opType) {
		case READ:
			return readMetadataOnly ? HttpMethod.HEAD : HttpMethod.GET;
		case STAT:
			return HttpMethod.HEAD;
		case DELETE:
			return HttpMethod.DELETE;
		default:
			return HttpMethod.PUT;
		}
	}

	protected abstract HttpMethod tokenHttpMethod(final OpType opType);

	protected abstract HttpMethod pathHttpMethod(final OpType opType);

	protected String dataUriPath(
					final I item, final String srcPath, final String dstPath, final OpType opType) {
		final String path = dstPath != null ? dstPath : srcPath;
		final String itemName = item.name();
		if (path == null || ItemNamePaths.holdsPath(itemName, path)) {
			return itemName.startsWith(SLASH) ? itemName : SLASH + itemName;
		}
		return ItemNamePaths.join(path.startsWith(SLASH) ? path : SLASH + path, itemName);
	}

	/**
	 * The source a copy reads, as {@link Operation#copySource()} keeps it: on the first attempt, what
	 * {@link #copySource(Operation)} resolves, which is then kept; on every later attempt, the kept
	 * value, so a copy sent again reads the object its first attempt read, whatever the item's name has
	 * become since.
	 */
	protected final String keptCopySource(final O op) {
		final String kept = op.copySource();
		if (kept != null) {
			return kept;
		}
		final String copySource = copySource(op);
		op.copySource(copySource);
		return copySource;
	}

	/**
	 * The URI path of the object a copy reads on its first attempt, which {@link #applyCopyHeaders}
	 * receives: the {@link #dataUriPath} of the item with no destination path, through any override of
	 * it. Called once per copy, while the item has its first name; see {@link Operation#copySource()}.
	 */
	@Override
	@SuppressWarnings("unchecked")
	protected String copySource(final O op) {
		return dataUriPath((I) op.item(), op.srcPath(), null, op.type());
	}

	protected abstract String tokenUriPath(
					final I item, final String srcPath, final String dstPath, final OpType opType);

	protected abstract String pathUriPath(
					final I item, final String srcPath, final String dstPath, final OpType opType);

	private static final ThreadLocal<StringBuilder> THR_LOC_RANGES_BUILDER = ThreadLocal.withInitial(StringBuilder::new);

	protected void applyRangesHeaders(final HttpHeaders httpHeaders, final DataOperation dataOp) {
		final long baseItemSize;
		try {
			baseItemSize = dataOp.item().size();
		} catch (final IOException e) {
			throw new AssertionError(e);
		}
		final List<Range> fixedRanges = dataOp.fixedRanges();
		final StringBuilder strb = THR_LOC_RANGES_BUILDER.get();
		strb.setLength(0);
		if (fixedRanges == null || fixedRanges.isEmpty()) {
			final BitSet rangesMaskPair[] = dataOp.markedRangesMaskPair();
			if (rangesMaskPair[0].isEmpty() && rangesMaskPair[1].isEmpty()) {
				return; // do not set the ranges header
			}
			// current layer first
			for (int i = 0; i < rangeCount(baseItemSize); i++) {
				if (rangesMaskPair[0].get(i)) {
					if (strb.length() > 0) {
						strb.append(',');
					}
					strb.append(rangeOffset(i))
									.append('-')
									.append(Math.min(rangeOffset(i + 1), baseItemSize) - 1);
				}
			}
			// then next layer ranges if any
			for (int i = 0; i < rangeCount(baseItemSize); i++) {
				if (rangesMaskPair[1].get(i)) {
					if (strb.length() > 0) {
						strb.append(',');
					}
					strb.append(rangeOffset(i))
									.append('-')
									.append(Math.min(rangeOffset(i + 1), baseItemSize) - 1);
				}
			}
		} else { // fixed byte ranges
			rangeListToStringBuff(fixedRanges, baseItemSize, strb);
		}
		httpHeaders.set(HttpHeaderNames.RANGE, "bytes=" + strb.toString());
	}

	protected static void rangeListToStringBuff(
					final List<Range> ranges, final long baseLength, final StringBuilder dstBuff) {
		Range nextFixedRange;
		long nextRangeSize;
		for (int i = 0; i < ranges.size(); i++) {
			nextFixedRange = ranges.get(i);
			nextRangeSize = nextFixedRange.getSize();
			if (i > 0) {
				dstBuff.append(',');
			}
			if (nextRangeSize == -1) {
				dstBuff.append(nextFixedRange.toString());
			} else {
				dstBuff.append(baseLength).append("-");
			}
		}
	}

	protected void applySharedHeaders(final HttpHeaders httpHeaders) {
		for (final var sharedHeader : sharedHeaders) {
			httpHeaders.add(sharedHeader.getKey(), sharedHeader.getValue());
		}
	}

	protected void applyDynamicHeaders(final HttpHeaders httpHeaders) {
		String headerName;
		String headerValue;
		Input<String> headerNameInput;
		Input<String> headerValueInput;
		for (final var nextHeader : dynamicHeaders.entrySet()) {
			headerName = nextHeader.getKey();
			// header name is a generator pattern
			headerNameInput = headerNameInputs.computeIfAbsent(headerName, HttpStorageDriverBase::expressionInput);
			if (headerNameInput == null) {
				continue;
			}
			// spin while header name generator is not ready
			headerName = headerNameInput.get();
			headerValue = nextHeader.getValue();
			// header value is a generator pattern
			headerValueInput = headerValueInputs.computeIfAbsent(headerValue, HttpStorageDriverBase::expressionInput);
			if (headerValueInput == null) {
				continue;
			}
			// spin while header value generator is not ready
			headerValue = headerValueInput.get();
			// put the generated header value into the request
			httpHeaders.set(headerName, headerValue);
		}
	}

	protected final String uriQuery() {
		return uriQueryInput.get();
	}

	protected abstract void applyChecksum(final HttpHeaders httpHeaders, final O op);

	protected abstract void applyMetaDataHeaders(final HttpHeaders httpHeaders);

	/** Operation-aware metadata hook retaining the legacy overload for extensions. */
	protected void applyMetaDataHeaders(final HttpHeaders httpHeaders, final O op) {
		applyMetaDataHeaders(httpHeaders);
	}

	protected abstract void applyAuthHeaders(
					final HttpHeaders httpHeaders,
					final HttpMethod httpMethod,
					final String dstUriPath,
					final Credential credential);

	protected abstract void applyCopyHeaders(final HttpHeaders httpHeaders, final String srcPath)
					throws URISyntaxException;

	/** Associates transport-specific attempt state with the leased request channel. */
	protected void bindRequestChannel(final Channel channel, final O op) {}

	/** Marks transport-specific attempt state eligible for response timeout handling. */
	protected void onRequestDispatched(final Channel channel, final O op) {}

	@Override
	protected final void sendRequest(final Channel channel, final O op) {
		final var nodeAddr = op.nodeAddr();
		try {
			if (channel != null) {
				bindRequestChannel(channel, op);
			}
			final var httpRequest = httpRequest(op, nodeAddr);
			if (channel == null) {
				return;
			} else {
				channel.write(httpRequest).addListener(httpReqSentCallback);
				onRequestDispatched(channel, op);
				if (op.integrityMetadata() != null) {
					markIntegrityRequestDispatched();
				}
				if (Loggers.MSG.isTraceEnabled()) {
					Loggers.MSG.trace(
									"{} >>>> {} {}", op.hashCode(), httpRequest.method(), httpRequest.uri());
				}
			}
			if (!(httpRequest instanceof FullHttpRequest)) {
				sendRequestData(channel, op);
			}
		} catch (final IOException e) {
			LogUtil.exception(Level.WARN, e, "Failed to write the data");
		} catch (final URISyntaxException e) {
			LogUtil.exception(Level.WARN, e, "Failed to build the request URI");
		} catch (final Throwable e) {
			throwUncheckedIfInterrupted(e);
			if (completeAfterRequestBuildFailure(channel, op, e)) {
				return;
			}
			logRequestBuildFailure(e);
		}
		channel.write(LastHttpContent.EMPTY_LAST_CONTENT).addListener(reqSentCallback);
		channel.flush();
	}

	/**
	 * Thrown by {@link #httpRequest} for an operation it can't build as asked, for example a versioned
	 * request whose {@code dataUriPath} override drops the version. Nothing is sent: the operation
	 * fails at once with {@link Status#FAIL_UNKNOWN}, and {@link #complete} returns its permit and
	 * closes its connection, as for any failed operation.
	 */
	protected static final class RejectedRequestException extends IllegalStateException {

		private static final long serialVersionUID = 1L;

		public RejectedRequestException(final String message) {
			super(message);
		}
	}

	/**
	 * Completes the operation at once after its request failed to build, when the failure has a
	 * terminal outcome: a reconciled standalone DELETE keeps its protocol status, and a {@link
	 * RejectedRequestException} on a leased channel fails with {@code FAIL_UNKNOWN}. Returns {@code
	 * false} for any other failure, which is only logged.
	 */
	private boolean completeAfterRequestBuildFailure(
					final Channel channel, final O op, final Throwable requestFailure) {
		final boolean reconciledDelete = op instanceof DeleteRequestOperation deleteOperation
						&& deleteOperation.deleteResult() != null;
		if (!reconciledDelete && !(channel != null && requestFailure instanceof RejectedRequestException)) {
			return false;
		}
		logRequestBuildFailure(requestFailure);
		if (!reconciledDelete) {
			op.status(Status.FAIL_UNKNOWN);
		}
		try {
			op.finishRequest();
		} catch (final IllegalStateException e) {
			LogUtil.exception(Level.DEBUG, e, "{}: invalid load operation request state", op.toString());
		}
		if (!reconciledDelete && !channel.eventLoop().inEventLoop()) {
			// Completing a failed operation closes its channel. Off the event loop the close is only
			// queued, and the pool could lease the closing channel to the next operation, as it never
			// does after a failed response. So complete on the event loop, as a response would.
			try {
				channel.eventLoop().execute(() -> completeBuildFailure(channel, op));
				return true;
			} catch (final RejectedExecutionException e) {
				LogUtil.exception(Level.DEBUG, e, "{}: completing on the caller's thread", op.toString());
			}
		}
		completeBuildFailure(channel, op);
		return true;
	}

	private void completeBuildFailure(final Channel channel, final O op) {
		try {
			complete(channel, op);
		} catch (final Throwable completionFailure) {
			throwUncheckedIfInterrupted(completionFailure);
			// Driver completion releases transport ownership before result publication. Keep the
			// reconciled protocol status if an extension-facing output rejects the terminal result.
			LogUtil.exception(
							Level.DEBUG, completionFailure, "Load operation result publication failure");
		}
	}

	private void logRequestBuildFailure(final Throwable failure) {
		if (isStopped() || isClosed()) {
			return;
		}
		// A malformed standalone batch can fail once per logical operation. Keep the first
		// diagnostic actionable without flooding the error log during a sustained failure.
		if (requestBuildFailureLogged == null
						|| requestBuildFailureLogged.compareAndSet(false, true)) {
			LogUtil.trace(Loggers.ERR, Level.ERROR, failure, "Send HTTP request failure");
		} else {
			LogUtil.trace(Loggers.ERR, Level.DEBUG, failure, "Send HTTP request failure");
		}
	}

	void sendHttpRequestComplete(final ChannelFuture future) {
		try {
			future.get(1, TimeUnit.NANOSECONDS);
		} catch (final ExecutionException | TimeoutException e) {
			final var cause = e.getCause();
			LogUtil.trace(Loggers.ERR, Level.WARN, cause, "Failed to send the request");
			final Object rawOp = future.channel().attr(ATTR_KEY_OPERATION).get();
			if (rawOp != null && !(rawOp instanceof Operation)) {
				LogUtil.trace(Loggers.ERR, Level.ERROR, cause,
								"sendHttpRequestComplete: ATTR_KEY_OPERATION contains {} instead of Operation",
								rawOp.getClass().getName());
				return;
			}
			final var op = (Operation) rawOp;
			op.status(Status.FAIL_IO);
			complete(future.channel(), (O) op);
		} catch (final InterruptedException e) {
			throwUnchecked(e);
		}
	};

	@Override
	protected void doClose() throws IOException {
		super.doClose();
		try {
			uriQueryInput.close();
		} catch (final Exception ignored) {
			// Best-effort shutdown must continue closing the remaining expression inputs.
		}
		sharedHeaders.clear();
		dynamicHeaders.clear();
		headerNameInputs
						.values()
						.forEach(
										headerNameInput -> {
											try {
												headerNameInput.close();
											} catch (final Exception ignored) {
												// Best-effort shutdown must continue closing the remaining expression inputs.
											}
										});
		headerNameInputs.clear();
		headerValueInputs
						.values()
						.forEach(
										headerValueInput -> {
											try {
												headerValueInput.close();
											} catch (final Exception ignored) {
												// Best-effort shutdown must continue closing the remaining expression inputs.
											}
										});
		headerValueInputs.clear();
	}
}
