package com.dell.spt.storage.driver.coop.aws.s3;

import static software.amazon.awssdk.crtcore.CrtConfigurationUtils.resolveHttpMonitoringOptions;
import static software.amazon.awssdk.crtcore.CrtConfigurationUtils.resolveProxy;
import static software.amazon.awssdk.http.crt.internal.AwsCrtConfigurationUtils.buildSocketOptions;
import static software.amazon.awssdk.http.crt.internal.AwsCrtConfigurationUtils.resolveCipherPreference;
import static software.amazon.awssdk.utils.FunctionalUtils.invokeSafely;

import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import java.net.URI;
import java.time.Duration;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import software.amazon.awssdk.crt.CrtResource;
import software.amazon.awssdk.crt.CrtRuntimeException;
import software.amazon.awssdk.crt.http.HttpClientConnection;
import software.amazon.awssdk.crt.http.HttpClientConnectionManager;
import software.amazon.awssdk.crt.http.HttpClientConnectionManagerOptions;
import software.amazon.awssdk.crt.http.HttpHeader;
import software.amazon.awssdk.crt.http.HttpManagerMetrics;
import software.amazon.awssdk.crt.http.HttpMonitoringOptions;
import software.amazon.awssdk.crt.http.HttpProxyOptions;
import software.amazon.awssdk.crt.http.HttpRequestBase;
import software.amazon.awssdk.crt.http.HttpStreamBase;
import software.amazon.awssdk.crt.http.HttpStreamBaseResponseHandler;
import software.amazon.awssdk.crt.http.HttpStreamMetrics;
import software.amazon.awssdk.crt.io.ClientBootstrap;
import software.amazon.awssdk.crt.io.SocketOptions;
import software.amazon.awssdk.crt.io.TlsContext;
import software.amazon.awssdk.crt.io.TlsContextOptions;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.SdkCancellationException;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.SdkHttpExecutionAttribute;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler;
import software.amazon.awssdk.http.crt.internal.AwsCrtClientBuilderBase;
import software.amazon.awssdk.http.crt.internal.CrtAsyncRequestContext;
import software.amazon.awssdk.http.crt.internal.CrtUtils;
import software.amazon.awssdk.http.crt.internal.request.CrtRequestAdapter;
import software.amazon.awssdk.http.crt.internal.response.CrtResponseAdapter;
import software.amazon.awssdk.metrics.MetricCollector;
import software.amazon.awssdk.metrics.NoOpMetricCollector;
import software.amazon.awssdk.utils.AttributeMap;
import software.amazon.awssdk.utils.IoUtils;
import software.amazon.awssdk.utils.Logger;
import software.amazon.awssdk.utils.NumericUtils;
import software.amazon.awssdk.utils.Validate;
import software.amazon.awssdk.utils.uri.SdkUri;

/**
 * AWS CRT async client exposing native stream timestamps for standalone DELETE timing.
 *
 * <p>The pinned CRT {@code Http1StreamManager} wraps the supplied response handler without forwarding
 * {@code onMetrics}. This client therefore owns the equivalent public connection-manager lifecycle
 * directly, allowing send/receive timestamps to reach the operation before response completion.
 */
final class CrtDeleteTimingAsyncHttpClient implements SdkAsyncHttpClient {

	private static final Logger LOG = Logger.loggerFor(CrtDeleteTimingAsyncHttpClient.class);
	private static final String CLIENT_NAME = "AwsCommonRuntime";
	private static final long DEFAULT_STREAM_WINDOW_SIZE = 16L * 1024L * 1024L;

	private final Map<URI, HttpClientConnectionManager> connectionPools = new ConcurrentHashMap<>();
	private final Map<CompletableFuture<Void>, SdkAsyncHttpResponseHandler> inFlightRequests = new ConcurrentHashMap<>();
	private final LinkedList<CrtResource> ownedSubResources = new LinkedList<>();
	private final HandlerFailureLogGuard handlerFailureLogGuard = new HandlerFailureLogGuard();
	private final ClientBootstrap bootstrap;
	private final SocketOptions socketOptions;
	private final TlsContext tlsContext;
	private final HttpProxyOptions proxyOptions;
	private final HttpMonitoringOptions monitoringOptions;
	private final long readBufferSize;
	private final long maxConnectionIdleMilliseconds;
	private final long connectionAcquisitionTimeoutMilliseconds;
	private final int maxConnections;
	private final SdkHttpExecutionAttribute<DeleteRequestOperation> operationAttribute;
	private boolean closed;

	private CrtDeleteTimingAsyncHttpClient(
					final Builder builder, final AttributeMap config) {
		operationAttribute = Objects.requireNonNull(
						builder.operationAttribute, "operationAttribute");
		readBufferSize = builder.getReadBufferSizeInBytes() == null
						? DEFAULT_STREAM_WINDOW_SIZE
						: builder.getReadBufferSizeInBytes();
		maxConnections = config.get(SdkHttpConfigurationOption.MAX_CONNECTIONS);
		monitoringOptions = resolveHttpMonitoringOptions(builder.getConnectionHealthConfiguration())
						.orElse(null);
		maxConnectionIdleMilliseconds = config
						.get(SdkHttpConfigurationOption.CONNECTION_MAX_IDLE_TIMEOUT)
						.toMillis();
		connectionAcquisitionTimeoutMilliseconds = config
						.get(SdkHttpConfigurationOption.CONNECTION_ACQUIRE_TIMEOUT)
						.toMillis();

		final var constructionResources = new ResourceStack();
		final ClientBootstrap clientBootstrap;
		final SocketOptions clientSocketOptions;
		final TlsContext clientTlsContext;
		final HttpProxyOptions clientProxyOptions;
		try {
			clientBootstrap = constructionResources.own(new ClientBootstrap(null, null));
			clientSocketOptions = constructionResources.own(buildSocketOptions(
							builder.getTcpKeepAliveConfiguration(),
							config.get(SdkHttpConfigurationOption.CONNECTION_TIMEOUT)));
			final TlsContextOptions clientTlsContextOptions = constructionResources.own(
							TlsContextOptions.createDefaultClient()
											.withCipherPreference(resolveCipherPreference(builder.getPostQuantumTlsEnabled()))
											.withVerifyPeer(!config.get(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES)));
			clientTlsContext = constructionResources.own(new TlsContext(clientTlsContextOptions));
			clientProxyOptions = resolveProxy(
							builder.getProxyConfiguration(), clientTlsContext).orElse(null);
		} catch (final RuntimeException | Error failure) {
			constructionResources.closeOnFailure(failure);
			throw failure;
		}
		bootstrap = clientBootstrap;
		socketOptions = clientSocketOptions;
		tlsContext = clientTlsContext;
		proxyOptions = clientProxyOptions;
		constructionResources.transferTo(ownedSubResources);
	}

	static Builder builder() {
		return new Builder();
	}

	@Override
	public String clientName() {
		return CLIENT_NAME;
	}

	@Override
	public CompletableFuture<Void> execute(final AsyncExecuteRequest asyncRequest) {
		Validate.paramNotNull(asyncRequest, "asyncRequest");
		Validate.paramNotNull(asyncRequest.request(), "SdkHttpRequest");
		Validate.paramNotNull(asyncRequest.requestContentPublisher(), "RequestContentPublisher");
		Validate.paramNotNull(asyncRequest.responseHandler(), "ResponseHandler");
		asyncRequest.metricCollector().ifPresent(
						collector -> collector.reportMetric(HttpMetric.HTTP_CLIENT_NAME, clientName()));

		final DeleteRequestOperation operation = asyncRequest.httpExecutionAttributes()
						.getAttribute(operationAttribute);
		final var context = CrtAsyncRequestContext.builder()
						.readBufferSize(readBufferSize)
						.request(asyncRequest)
						.protocol(Protocol.HTTP1_1)
						.build();
		final HttpRequestBase crtRequest;
		try {
			crtRequest = CrtRequestAdapter.toAsyncCrtRequest(context);
		} catch (Throwable error) {
			final var completion = createExecutionFuture(asyncRequest);
			reportAsyncFailure(error, completion, asyncRequest.responseHandler());
			return completion;
		}
		return executeRequest(asyncRequest, crtRequest, operation);
	}

	private CompletableFuture<Void> executeRequest(
					final AsyncExecuteRequest request,
					final HttpRequestBase crtRequest,
					final DeleteRequestOperation operation) {
		final var completion = createExecutionFuture(request);
		final CrtResponseAdapter responseAdapter = CrtResponseAdapter.toCrtResponseHandler(
						completion, request.responseHandler());
		completion.whenComplete((ignored, error) -> {
			inFlightRequests.remove(completion);
			if (error != null) {
				responseAdapter.closeConnection();
			}
		});

		final HttpClientConnectionManager connectionManager = registerExecution(
						poolKey(request), completion, request.responseHandler());
		final MetricCollector metricCollector = request.metricCollector().orElse(null);
		final boolean reportMetrics = metricCollector != null
						&& !(metricCollector instanceof NoOpMetricCollector);
		final long acquisitionStartNanos = reportMetrics ? System.nanoTime() : 0;
		connectionManager.acquireConnection().whenComplete((connection, error) -> {
			if (reportMetrics) {
				reportManagerMetrics(connectionManager, metricCollector, acquisitionStartNanos);
			}
			if (completion.isDone()) {
				if (connection != null) {
					connectionManager.releaseConnection(connection);
				}
				return;
			}
			if (error != null) {
				reportAsyncFailure(
								CrtUtils.wrapCrtException(error), completion, request.responseHandler());
				return;
			}
			activateStream(
							connectionManager,
							connection,
							crtRequest,
							responseAdapter,
							operation,
							completion,
							request.responseHandler());
		});
		return completion;
	}

	private void activateStream(
					final HttpClientConnectionManager connectionManager,
					final HttpClientConnection connection,
					final HttpRequestBase request,
					final CrtResponseAdapter responseAdapter,
					final DeleteRequestOperation operation,
					final CompletableFuture<Void> completion,
					final SdkAsyncHttpResponseHandler sdkResponseHandler) {
		final var responseHandler = new TimingResponseHandler(
						responseAdapter, operation, connectionManager, connection);
		try {
			final HttpStreamBase stream = connection.makeRequest(request, responseHandler, false);
			responseAdapter.onAcquireStream(stream);
			try {
				stream.activate();
			} catch (CrtRuntimeException error) {
				responseHandler.onResponseComplete(stream, error.errorCode);
			}
		} catch (Throwable error) {
			responseHandler.releaseConnection();
			reportAsyncFailure(
							CrtUtils.wrapCrtException(error), completion, sdkResponseHandler);
		}
	}

	private static CompletableFuture<Void> createExecutionFuture(
					final AsyncExecuteRequest request) {
		final var completion = new CompletableFuture<Void>();
		completion.whenComplete((ignored, error) -> {
			if (error != null && completion.isCancelled()) {
				request.responseHandler().onError(
								new SdkCancellationException("The request was cancelled"));
			}
		});
		return completion;
	}

	private void reportAsyncFailure(
					final Throwable error,
					final CompletableFuture<Void> completion,
					final SdkAsyncHttpResponseHandler responseHandler) {
		try {
			responseHandler.onError(error);
		} catch (Exception handlerError) {
			logResponseHandlerFailure(handlerError);
		}
		completion.completeExceptionally(error);
	}

	private void logResponseHandlerFailure(final Exception failure) {
		if (handlerFailureLogGuard.claimHighSeverity()) {
			LOG.error(
							() -> "SdkAsyncHttpResponseHandler threw from onError; ignoring it; "
											+ "further handler failures are logged at DEBUG",
							failure);
		} else {
			LOG.debug(
							() -> "SdkAsyncHttpResponseHandler threw from onError; ignoring it",
							failure);
		}
	}

	private static void reportManagerMetrics(
					final HttpClientConnectionManager connectionManager,
					final MetricCollector metricCollector,
					final long acquisitionStartNanos) {
		metricCollector.reportMetric(
						HttpMetric.CONCURRENCY_ACQUIRE_DURATION,
						Duration.ofNanos(System.nanoTime() - acquisitionStartNanos));
		final HttpManagerMetrics metrics = connectionManager.getManagerMetrics();
		metricCollector.reportMetric(HttpMetric.MAX_CONCURRENCY, connectionManager.getMaxConnections());
		metricCollector.reportMetric(
						HttpMetric.AVAILABLE_CONCURRENCY,
						NumericUtils.saturatedCast(metrics.getAvailableConcurrency()));
		metricCollector.reportMetric(
						HttpMetric.LEASED_CONCURRENCY,
						NumericUtils.saturatedCast(metrics.getLeasedConcurrency()));
		metricCollector.reportMetric(
						HttpMetric.PENDING_CONCURRENCY_ACQUIRES,
						NumericUtils.saturatedCast(metrics.getPendingConcurrencyAcquires()));
	}

	private synchronized HttpClientConnectionManager registerExecution(
					final URI uri,
					final CompletableFuture<Void> completion,
					final SdkAsyncHttpResponseHandler responseHandler) {
		if (closed) {
			throw new IllegalStateException(
							"Client is closed. No more requests can be made with this client.");
		}
		inFlightRequests.put(completion, responseHandler);
		try {
			return connectionPools.computeIfAbsent(uri, this::createConnectionPool);
		} catch (RuntimeException | Error error) {
			inFlightRequests.remove(completion);
			throw error;
		}
	}

	private HttpClientConnectionManager createConnectionPool(final URI uri) {
		LOG.debug(() -> String.format(
						"Creating DELETE timing connection pool for URI:%s, MaxConns:%d",
						uri,
						maxConnections));
		final boolean secure = "https".equalsIgnoreCase(uri.getScheme());
		final var options = new HttpClientConnectionManagerOptions()
						.withClientBootstrap(bootstrap)
						.withSocketOptions(socketOptions)
						.withTlsContext(secure ? tlsContext : null)
						.withUri(uri)
						.withWindowSize(readBufferSize)
						.withMaxConnections(maxConnections)
						.withManualWindowManagement(true)
						.withProxyOptions(proxyOptions)
						.withMonitoringOptions(monitoringOptions)
						.withMaxConnectionIdleInMilliseconds(maxConnectionIdleMilliseconds)
						.withConnectionAcquisitionTimeoutInMilliseconds(
										connectionAcquisitionTimeoutMilliseconds);
		return HttpClientConnectionManager.create(options);
	}

	private static URI poolKey(final AsyncExecuteRequest request) {
		return invokeSafely(() -> SdkUri.getInstance().newUri(
						request.request().protocol(),
						null,
						request.request().host(),
						request.request().port(),
						null,
						null,
						null));
	}

	/** Transactional reverse-order ownership for partially constructed native resources. */
	static final class ResourceStack {
		private final LinkedList<CrtResource> resources = new LinkedList<>();

		<T extends CrtResource> T own(final T resource) {
			if (resource != null) {
				resources.push(resource);
			}
			return resource;
		}

		void transferTo(final LinkedList<CrtResource> destination) {
			destination.addAll(resources);
			resources.clear();
		}

		void closeOnFailure(final Throwable constructionFailure) {
			resources.forEach(resource -> {
				try {
					resource.close();
				} catch (final RuntimeException | Error closeFailure) {
					constructionFailure.addSuppressed(closeFailure);
				}
			});
			resources.clear();
		}
	}

	/** Bounds high-severity logging across every per-request response-handler failure path. */
	static final class HandlerFailureLogGuard {
		private final AtomicBoolean highSeverityClaimed = new AtomicBoolean();

		boolean claimHighSeverity() {
			return highSeverityClaimed.compareAndSet(false, true);
		}
	}

	@Override
	public void close() {
		final Map<CompletableFuture<Void>, SdkAsyncHttpResponseHandler> requestsToTerminate;
		final Map<URI, HttpClientConnectionManager> poolsToClose;
		final LinkedList<CrtResource> resourcesToClose;
		synchronized (this) {
			if (closed) {
				return;
			}
			closed = true;
			requestsToTerminate = Map.copyOf(inFlightRequests);
			poolsToClose = Map.copyOf(connectionPools);
			resourcesToClose = new LinkedList<>(ownedSubResources);
			connectionPools.clear();
			ownedSubResources.clear();
		}
		requestsToTerminate.forEach((completion, handler) -> {
			final var error = new SdkCancellationException(
							"The HTTP client was closed with a request in flight");
			if (completion.completeExceptionally(error)) {
				notifyAsyncFailure(error, handler);
			}
		});
		poolsToClose.values().forEach(pool -> IoUtils.closeQuietly(pool, LOG.logger()));
		resourcesToClose.forEach(resource -> IoUtils.closeQuietly(resource, LOG.logger()));
	}

	private void notifyAsyncFailure(
					final Throwable error, final SdkAsyncHttpResponseHandler responseHandler) {
		try {
			responseHandler.onError(error);
		} catch (Exception handlerError) {
			logResponseHandlerFailure(handlerError);
		}
	}

	static final class TimingResponseHandler implements HttpStreamBaseResponseHandler {

		private final CrtResponseAdapter delegate;
		private final DeleteRequestOperation operation;
		private final HttpClientConnectionManager connectionManager;
		private final HttpClientConnection connection;
		private final AtomicBoolean connectionReleased = new AtomicBoolean();

		TimingResponseHandler(
						final CrtResponseAdapter delegate,
						final DeleteRequestOperation operation,
						final HttpClientConnectionManager connectionManager,
						final HttpClientConnection connection) {
			this.delegate = delegate;
			this.operation = operation;
			this.connectionManager = connectionManager;
			this.connection = connection;
		}

		@Override
		public void onResponseHeaders(
						final HttpStreamBase stream,
						final int responseStatusCode,
						final int blockType,
						final HttpHeader[] nextHeaders) {
			delegate.onResponseHeaders(stream, responseStatusCode, blockType, nextHeaders);
		}

		@Override
		public void onResponseHeadersDone(
						final HttpStreamBase stream, final int blockType) {
			delegate.onResponseHeadersDone(stream, blockType);
		}

		@Override
		public int onResponseBody(final HttpStreamBase stream, final byte[] bodyBytesIn) {
			return delegate.onResponseBody(stream, bodyBytesIn);
		}

		@Override
		public void onMetrics(final HttpStreamBase stream, final HttpStreamMetrics metrics) {
			if (operation != null) {
				operation.recordTransportRequestTiming(
								metrics.getSendStartTimestampNs(), metrics.getReceiveStartTimestampNs());
			}
			delegate.onMetrics(stream, metrics);
		}

		@Override
		public void onResponseComplete(final HttpStreamBase stream, final int errorCode) {
			try {
				delegate.onResponseComplete(stream, errorCode);
			} finally {
				releaseConnection();
			}
		}

		void releaseConnection() {
			if (connectionReleased.compareAndSet(false, true)) {
				connectionManager.releaseConnection(connection);
			}
		}
	}

	static final class Builder extends AwsCrtClientBuilderBase<Builder> {

		private SdkHttpExecutionAttribute<DeleteRequestOperation> operationAttribute;

		private Builder() {}

		Builder operationAttribute(
						final SdkHttpExecutionAttribute<DeleteRequestOperation> value) {
			operationAttribute = Objects.requireNonNull(value);
			return this;
		}

		SdkAsyncHttpClient build() {
			final AttributeMap options = getAttributeMap().build()
							.merge(SdkHttpConfigurationOption.GLOBAL_HTTP_DEFAULTS);
			return new CrtDeleteTimingAsyncHttpClient(this, options);
		}
	}
}
