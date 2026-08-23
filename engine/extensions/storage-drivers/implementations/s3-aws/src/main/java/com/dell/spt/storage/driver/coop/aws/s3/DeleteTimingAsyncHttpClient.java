package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.http.SdkHttpExecutionAttribute;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler;

/** Marks standalone DELETE timing at the AWS HTTP transport boundary. */
final class DeleteTimingAsyncHttpClient implements SdkAsyncHttpClient {

	static final SdkHttpExecutionAttribute<DeleteRequestOperation> DELETE_OPERATION = new SdkHttpExecutionAttribute<>(DeleteRequestOperation.class) {};

	private final SdkAsyncHttpClient delegate;

	DeleteTimingAsyncHttpClient(final SdkAsyncHttpClient delegate) {
		this.delegate = Objects.requireNonNull(delegate);
	}

	@Override
	public CompletableFuture<Void> execute(final AsyncExecuteRequest request) {
		final DeleteRequestOperation operation = request.httpExecutionAttributes()
						.getAttribute(DELETE_OPERATION);
		if (operation == null) {
			return delegate.execute(request);
		}
		operation.beginTransportAttempt();

		final var builder = AsyncExecuteRequest.builder()
						.request(request.request())
						.responseHandler(new TimingResponseHandler(request.responseHandler(), operation))
						.fullDuplex(request.fullDuplex())
						.httpExecutionAttributes(request.httpExecutionAttributes())
						.requestContentPublisher(request.requestContentPublisher());
		request.metricCollector().ifPresent(builder::metricCollector);
		return delegate.execute(builder.build());
	}

	@Override
	public String clientName() {
		return delegate.clientName();
	}

	@Override
	public void close() {
		delegate.close();
	}

	private static final class TimingResponseHandler implements SdkAsyncHttpResponseHandler {
		private final SdkAsyncHttpResponseHandler delegate;
		private final DeleteRequestOperation operation;

		private TimingResponseHandler(
						final SdkAsyncHttpResponseHandler delegate,
						final DeleteRequestOperation operation) {
			this.delegate = delegate;
			this.operation = operation;
		}

		@Override
		public void onHeaders(final SdkHttpResponse response) {
			operation.markResponseFirstByteReceived();
			delegate.onHeaders(response);
		}

		@Override
		public void onStream(final Publisher<ByteBuffer> publisher) {
			delegate.onStream(new TimingResponsePublisher(publisher, operation));
		}

		@Override
		public void onError(final Throwable error) {
			operation.failTransportAttempt();
			delegate.onError(error);
		}
	}

	private static final class TimingResponsePublisher implements Publisher<ByteBuffer> {
		private final Publisher<ByteBuffer> delegate;
		private final DeleteRequestOperation operation;
		private final AtomicBoolean completed = new AtomicBoolean();

		private TimingResponsePublisher(
						final Publisher<ByteBuffer> delegate,
						final DeleteRequestOperation operation) {
			this.delegate = delegate;
			this.operation = operation;
		}

		@Override
		public void subscribe(final Subscriber<? super ByteBuffer> subscriber) {
			delegate.subscribe(new Subscriber<>() {
				@Override
				public void onSubscribe(final Subscription subscription) {
					subscriber.onSubscribe(subscription);
				}

				@Override
				public void onNext(final ByteBuffer buffer) {
					subscriber.onNext(buffer);
				}

				@Override
				public void onError(final Throwable error) {
					operation.failTransportAttempt();
					subscriber.onError(error);
				}

				@Override
				public void onComplete() {
					if (completed.compareAndSet(false, true)) {
						operation.markResponseLastByteReceived();
					}
					subscriber.onComplete();
				}
			});
		}
	}
}
