package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler;
import software.amazon.awssdk.http.async.SdkHttpContentPublisher;

class DeleteTimingAsyncHttpClientTest {

	@Test
	void requestBodyDeliveryIsNotAByteBoundaryAndStreamCompletionOwnsLastByte() {
		final DeleteRequestOperation op = S3AwsDeleteRequestTestFixture.operation(
						S3AwsDeleteRequestTestFixture.target("key", null));
		op.startRequest();
		op.finishRequest();
		final SdkAsyncHttpClient delegate = mock(SdkAsyncHttpClient.class);
		final SdkAsyncHttpResponseHandler responseHandler = mock(SdkAsyncHttpResponseHandler.class);
		final AtomicReference<AsyncExecuteRequest> delegated = new AtomicReference<>();
		when(delegate.execute(any(AsyncExecuteRequest.class))).thenAnswer(invocation -> {
			final AsyncExecuteRequest delegatedRequest = invocation.getArgument(0);
			delegated.set(delegatedRequest);
			assertEquals(0L, op.requestFirstByteTime(),
							"request handoff is earlier than delivery of the first request body byte");
			delegatedRequest.requestContentPublisher().subscribe(discardingSubscriber());
			assertEquals(0L, op.requestFirstByteTime(),
							"request-body delivery must not masquerade as a transport byte marker");
			assertEquals(0L, op.transportRequestLatency(),
							"only native CRT stream metrics may populate AWS request latency");
			return CompletableFuture.completedFuture(null);
		});
		final AsyncExecuteRequest request = AsyncExecuteRequest.builder()
						.request(SdkHttpRequest.builder()
										.method(SdkHttpMethod.DELETE)
										.uri(URI.create("http://127.0.0.1/bucket/key"))
										.build())
						.responseHandler(responseHandler)
						.requestContentPublisher(oneByteContent())
						.putHttpExecutionAttribute(
										DeleteTimingAsyncHttpClient.DELETE_OPERATION, op)
						.build();

		final var client = new DeleteTimingAsyncHttpClient(delegate);
		client.execute(request).join();
		assertEquals(0L, op.responseFirstByteTime());

		final SdkHttpResponse response = SdkHttpResponse.builder().statusCode(204).build();
		delegated.get().responseHandler().onHeaders(response);
		assertEquals(op.respTimeStart(), op.responseFirstByteTime());
		assertEquals(0L, op.respTimeDone(), "headers are not the last response byte");
		verify(responseHandler).onHeaders(response);

		delegated.get().responseHandler().onStream(emptyContent());
		final var streamCaptor = org.mockito.ArgumentCaptor.forClass(Publisher.class);
		verify(responseHandler).onStream(streamCaptor.capture());
		assertEquals(0L, op.respTimeDone(), "SDK handler has not consumed the response stream yet");
		streamCaptor.getValue().subscribe(discardingSubscriber());
		assertTrue(op.respTimeDone() >= op.responseFirstByteTime());
	}

	@Test
	void bodylessTransportErrorsClearIncompleteResponseTiming() {
		final DeleteRequestOperation op = S3AwsDeleteRequestTestFixture.operation(
						S3AwsDeleteRequestTestFixture.target("key", null));
		op.startRequest();
		op.finishRequest();
		final SdkAsyncHttpClient delegate = mock(SdkAsyncHttpClient.class);
		final SdkAsyncHttpResponseHandler responseHandler = mock(SdkAsyncHttpResponseHandler.class);
		final AtomicReference<AsyncExecuteRequest> delegated = new AtomicReference<>();
		when(delegate.execute(any(AsyncExecuteRequest.class))).thenAnswer(invocation -> {
			delegated.set(invocation.getArgument(0));
			return CompletableFuture.completedFuture(null);
		});
		final AsyncExecuteRequest request = AsyncExecuteRequest.builder()
						.request(SdkHttpRequest.builder()
										.method(SdkHttpMethod.DELETE)
										.uri(URI.create("http://127.0.0.1/bucket/key"))
										.build())
						.responseHandler(responseHandler)
						.requestContentPublisher(emptyContent())
						.putHttpExecutionAttribute(
										DeleteTimingAsyncHttpClient.DELETE_OPERATION, op)
						.build();

		new DeleteTimingAsyncHttpClient(delegate).execute(request).join();
		final IllegalStateException failure = new IllegalStateException("transport failed");
		delegated.get().responseHandler().onError(failure);

		assertEquals(0L, op.requestFirstByteTime());
		assertEquals(0L, op.transportRequestLatency());
		assertEquals(0L, op.responseFirstByteTime());
		assertEquals(0L, op.respTimeDone());
		verify(responseHandler).onError(failure);
	}

	@Test
	void completedRetryableResponseThenTerminalTransportErrorClearsStaleResponseTiming() {
		final DeleteRequestOperation op = S3AwsDeleteRequestTestFixture.operation(
						S3AwsDeleteRequestTestFixture.target("key", null));
		op.startRequest();
		op.finishRequest();
		final var attempts = new CopyOnWriteArrayList<AsyncExecuteRequest>();
		final SdkAsyncHttpClient delegate = mock(SdkAsyncHttpClient.class);
		when(delegate.execute(any(AsyncExecuteRequest.class))).thenAnswer(invocation -> {
			attempts.add(invocation.getArgument(0));
			return CompletableFuture.completedFuture(null);
		});
		final var client = new DeleteTimingAsyncHttpClient(delegate);
		final AsyncExecuteRequest request = request(op, consumingResponseHandler());

		client.execute(request).join();
		completeTransportResponse(attempts.get(0));
		assertTrue(op.respTimeDone() > 0);
		client.execute(request).join();
		assertEquals(0L, op.requestFirstByteTime(),
						"request-body delivery must not substitute for native stream metrics");
		assertEquals(0L, op.responseFirstByteTime());
		assertEquals(0L, op.respTimeDone());
		attempts.get(1).requestContentPublisher().subscribe(discardingSubscriber());
		attempts.get(1).responseHandler().onError(new IllegalStateException("terminal transport error"));
		assertEquals(0L, op.responseFirstByteTime());
		assertEquals(0L, op.respTimeDone(),
						"terminal failure must not reuse a completed retryable response");
	}

	@Test
	void completedRetryableResponseThenSuccessUsesFinalAttemptResponseTiming() throws Exception {
		final DeleteRequestOperation op = S3AwsDeleteRequestTestFixture.operation(
						S3AwsDeleteRequestTestFixture.target("key", null));
		op.startRequest();
		op.finishRequest();
		final var attempts = new CopyOnWriteArrayList<AsyncExecuteRequest>();
		final SdkAsyncHttpClient delegate = mock(SdkAsyncHttpClient.class);
		when(delegate.execute(any(AsyncExecuteRequest.class))).thenAnswer(invocation -> {
			attempts.add(invocation.getArgument(0));
			return CompletableFuture.completedFuture(null);
		});
		final var client = new DeleteTimingAsyncHttpClient(delegate);
		final AsyncExecuteRequest request = request(op, consumingResponseHandler());

		client.execute(request).join();
		completeTransportResponse(attempts.get(0));
		final long retryableResponseMarker = op.responseFirstByteTime();
		Thread.sleep(2);

		client.execute(request).join();
		completeTransportResponse(attempts.get(1));
		assertEquals(0L, op.requestFirstByteTime());
		assertTrue(op.responseFirstByteTime() > retryableResponseMarker,
						"successful retry must replace retryable-attempt response timing");
		assertTrue(op.respTimeDone() >= op.responseFirstByteTime());
	}

	private static AsyncExecuteRequest request(
					final DeleteRequestOperation op, final SdkAsyncHttpResponseHandler responseHandler) {
		return AsyncExecuteRequest.builder()
						.request(SdkHttpRequest.builder()
										.method(SdkHttpMethod.DELETE)
										.uri(URI.create("http://127.0.0.1/bucket/key"))
										.build())
						.responseHandler(responseHandler)
						.requestContentPublisher(oneByteContent())
						.putHttpExecutionAttribute(DeleteTimingAsyncHttpClient.DELETE_OPERATION, op)
						.build();
	}

	private static void completeTransportResponse(final AsyncExecuteRequest request) {
		request.requestContentPublisher().subscribe(discardingSubscriber());
		request.responseHandler().onHeaders(SdkHttpResponse.builder().statusCode(503).build());
		request.responseHandler().onStream(emptyContent());
	}

	private static SdkAsyncHttpResponseHandler consumingResponseHandler() {
		return new SdkAsyncHttpResponseHandler() {
			@Override
			public void onHeaders(final SdkHttpResponse response) {}

			@Override
			public void onStream(final Publisher<ByteBuffer> publisher) {
				publisher.subscribe(discardingSubscriber());
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

	private static SdkHttpContentPublisher oneByteContent() {
		return new SdkHttpContentPublisher() {
			@Override
			public Optional<Long> contentLength() {
				return Optional.of(1L);
			}

			@Override
			public void subscribe(final Subscriber<? super ByteBuffer> subscriber) {
				subscriber.onSubscribe(new Subscription() {
					@Override
					public void request(final long count) {
						subscriber.onNext(ByteBuffer.wrap(new byte[]{1
						}));
						subscriber.onComplete();
					}

					@Override
					public void cancel() {}
				});
			}
		};
	}

	private static Subscriber<ByteBuffer> discardingSubscriber() {
		return new Subscriber<>() {
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
		};
	}

}
