package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler;
import software.amazon.awssdk.http.async.SdkHttpContentPublisher;
import software.amazon.awssdk.http.crt.internal.response.CrtResponseAdapter;
import software.amazon.awssdk.crt.CrtResource;
import software.amazon.awssdk.crt.http.HttpClientConnection;
import software.amazon.awssdk.crt.http.HttpClientConnectionManager;
import software.amazon.awssdk.crt.http.HttpStreamBase;

/** Resource-lifecycle canaries for the native CRT DELETE timing bridge. */
final class CrtDeleteTimingAsyncHttpClientIntegrationTest {

	private static final long TIMEOUT_SECONDS = 3;

	private final ExecutorService executor = Executors.newCachedThreadPool();
	private volatile CountDownLatch heldRequestStarted;
	private volatile CountDownLatch releaseHeldRequest;
	private HttpServer server;

	@BeforeEach
	void startServer() throws IOException {
		heldRequestStarted = new CountDownLatch(1);
		releaseHeldRequest = new CountDownLatch(1);
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/", this::handle);
		server.setExecutor(executor);
		server.start();
	}

	@AfterEach
	void stopServer() {
		releaseHeldRequest.countDown();
		if (server != null) {
			server.stop(0);
		}
		executor.shutdownNow();
	}

	@Test
	void cancellationReleasesTheOnlyConnectionForTheNextRequest() throws Exception {
		try (final var client = newClient()) {
			final CompletableFuture<Void> held = execute(client, "/hold");
			assertTrue(heldRequestStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

			assertTrue(held.cancel(true));
			assertDoesNotThrow(() -> execute(client, "/ok").get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
		}
	}

	@Test
	void acquisitionTimeoutDoesNotLeakTheOnlyConnection() throws Exception {
		try (final var client = newClient()) {
			final CompletableFuture<Void> held = execute(client, "/hold");
			assertTrue(heldRequestStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

			assertThrows(ExecutionException.class,
							() -> execute(client, "/ok").get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
			assertTrue(held.cancel(true));
			assertDoesNotThrow(() -> execute(client, "/ok").get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
		}
	}

	@Test
	void prematureDisconnectReleasesTheOnlyConnectionForTheNextRequest() {
		try (final var client = newClient()) {
			assertThrows(ExecutionException.class,
							() -> execute(client, "/disconnect").get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
			assertDoesNotThrow(() -> execute(client, "/ok").get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
		}
	}

	@Test
	void closeWithAnInFlightRequestTerminatesItAndRejectsFurtherWork() throws Exception {
		final var client = newClient();
		final CompletableFuture<Void> held = execute(client, "/hold");
		assertTrue(heldRequestStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

		client.close();

		assertThrows(ExecutionException.class,
						() -> held.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
		assertThrows(IllegalStateException.class, () -> execute(client, "/ok"));
		client.close();
	}

	@Test
	void duplicateTerminalSignalsReleaseTheNativeConnectionExactlyOnce() {
		final HttpClientConnectionManager manager = mock(HttpClientConnectionManager.class);
		final HttpClientConnection connection = mock(HttpClientConnection.class);
		final CrtResponseAdapter delegate = mock(CrtResponseAdapter.class);
		final HttpStreamBase stream = mock(HttpStreamBase.class);
		final var handler = new CrtDeleteTimingAsyncHttpClient.TimingResponseHandler(
						delegate, null, manager, connection);

		handler.onResponseComplete(stream, 0);
		handler.onResponseComplete(stream, 1);

		verify(delegate).onResponseComplete(stream, 0);
		verify(delegate).onResponseComplete(stream, 1);
		verify(manager, times(1)).releaseConnection(connection);
	}

	@Test
	void constructionResourceStackClosesPartialNativeResourcesInReverseOrder() {
		final CrtResource first = mock(CrtResource.class);
		final CrtResource second = mock(CrtResource.class);
		final var resources = new CrtDeleteTimingAsyncHttpClient.ResourceStack();
		resources.own(first);
		resources.own(second);
		final var constructionFailure = new IllegalStateException("injected construction failure");

		resources.closeOnFailure(constructionFailure);

		final var order = inOrder(second, first);
		order.verify(second).close();
		order.verify(first).close();
	}

	@Test
	void repeatedResponseHandlerFailuresClaimHighSeverityLoggingOnlyOnce() {
		final var guard = new CrtDeleteTimingAsyncHttpClient.HandlerFailureLogGuard();

		assertTrue(guard.claimHighSeverity());
		assertFalse(guard.claimHighSeverity());
		assertFalse(guard.claimHighSeverity());
	}

	private DeleteTimingAsyncHttpClient newClient() {
		return new DeleteTimingAsyncHttpClient(
						CrtDeleteTimingAsyncHttpClient.builder()
										.operationAttribute(DeleteTimingAsyncHttpClient.DELETE_OPERATION)
										.maxConcurrency(1)
										.connectionAcquisitionTimeout(Duration.ofMillis(200))
										.build());
	}

	private CompletableFuture<Void> execute(
					final DeleteTimingAsyncHttpClient client, final String path) {
		final DeleteRequestOperation operation = S3AwsDeleteRequestTestFixture.operation(
						S3AwsDeleteRequestTestFixture.target(path.substring(1), null));
		operation.startRequest();
		operation.finishRequest();
		return client.execute(AsyncExecuteRequest.builder()
						.request(SdkHttpRequest.builder()
										.method(SdkHttpMethod.DELETE)
										.uri(URI.create("http://127.0.0.1:" + server.getAddress().getPort() + path))
										.build())
						.requestContentPublisher(emptyContent())
						.responseHandler(consumingResponseHandler())
						.putHttpExecutionAttribute(DeleteTimingAsyncHttpClient.DELETE_OPERATION, operation)
						.build());
	}

	private void handle(final HttpExchange exchange) throws IOException {
		if ("/disconnect".equals(exchange.getRequestURI().getPath())) {
			exchange.close();
			return;
		}
		if ("/hold".equals(exchange.getRequestURI().getPath())) {
			heldRequestStarted.countDown();
			try {
				releaseHeldRequest.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS);
			} catch (final InterruptedException error) {
				Thread.currentThread().interrupt();
			}
		}
		exchange.sendResponseHeaders(204, -1);
		exchange.close();
	}

	private static SdkAsyncHttpResponseHandler consumingResponseHandler() {
		return new SdkAsyncHttpResponseHandler() {
			@Override
			public void onHeaders(final SdkHttpResponse response) {}

			@Override
			public void onStream(final Publisher<ByteBuffer> publisher) {
				publisher.subscribe(new Subscriber<>() {
					@Override
					public void onSubscribe(final Subscription subscription) {
						subscription.request(Long.MAX_VALUE);
					}

					@Override
					public void onNext(final ByteBuffer buffer) {}

					@Override
					public void onError(final Throwable error) {}

					@Override
					public void onComplete() {}
				});
			}

			@Override
			public void onError(final Throwable error) {}
		};
	}

	private static SdkHttpContentPublisher emptyContent() {
		return new SdkHttpContentPublisher() {
			@Override
			public Optional<Long> contentLength() {
				return Optional.of(0L);
			}

			@Override
			public void subscribe(final Subscriber<? super ByteBuffer> subscriber) {
				subscriber.onSubscribe(new Subscription() {
					@Override
					public void request(final long count) {
						subscriber.onComplete();
					}

					@Override
					public void cancel() {}
				});
			}
		};
	}
}
