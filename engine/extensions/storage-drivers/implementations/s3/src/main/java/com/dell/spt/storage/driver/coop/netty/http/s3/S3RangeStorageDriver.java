package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.range.*;
import com.dell.spt.base.load.step.local.context.range.RangeReadRuntime;
import com.dell.spt.base.storage.driver.range.RangeReadDriverSupport;
import com.dell.spt.storage.driver.coop.range.RangeReadQueue;
import com.dell.spt.storage.driver.coop.netty.http.RangeReadResponseDecoder;
import com.dell.spt.storage.driver.coop.netty.http.RangeReadResponseHandler;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import java.net.ConnectException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Dedicated range transport; the ordinary S3 driver and its per-operation route remain unchanged. */
final class S3RangeStorageDriver extends S3StorageDriver<DataItem, RangeReadOperation<DataItem>>
				implements RangeReadDriverSupport {
	private record Flight(RangeReadOperation<DataItem> operation, RangeReadCirculation circulation,
					RangeReadAttempt attempt, Channel channel) {}

	// Set by the existing superclass construction hook, before this class's field initializers.
	private NonBlockingConnPool rangePool;
	private final RangeReadRuntime<DataItem> runtime;
	private final RangeReadQueue<DataItem> ranges;
	private final ConcurrentMap<RangeReadAttempt, Flight> flights = new ConcurrentHashMap<>();

	S3RangeStorageDriver(String stepId, DataInput input, Config storage, int batchSize, RangeReadPolicy policy)
					throws InterruptedException {
		super(stepId, input, validate(storage, policy), false, batchSize);
		runtime = new RangeReadRuntime<>(policy, Math.addExact(storage.intVal("driver-limit-queue-input"),
						Math.max(1, concurrencyLimit)), this, this::publishRetainedRangeResult);
		ranges = new RangeReadQueue<>(runtime);
		enableOperationLifecycle(runtime.tracker());
	}

	private static Config validate(Config storage, RangeReadPolicy policy) {
		Objects.requireNonNull(policy);
		if (!"none".equals(storage.stringVal("integrity-mode")))
			throw new IllegalConfigurationException("Range READ does not support integrity verification");
		if (storage.boolVal("object-tagging-enabled"))
			throw new IllegalConfigurationException("Range READ does not support object tagging operations");
		if (storage.boolVal("net-http-read-metadata-only"))
			throw new IllegalConfigurationException("Range READ requires an object body, not metadata-only READ");
		if (storage.intVal("net-timeoutMilliSec") <= 0)
			throw new IllegalConfigurationException("Range READ requires a positive storage.net.timeoutMilliSec");
		try {
			Math.addExact(storage.intVal("driver-limit-queue-input"), Math.max(1, storage.intVal("driver-limit-concurrency")));
		} catch (ArithmeticException invalid) {
			throw new IllegalConfigurationException("Range READ outstanding capacity exceeds signed-32-bit bounds", invalid);
		}
		return storage;
	}

	@Override
	protected NonBlockingConnPool createConnectionPool() {
		return rangePool = super.createConnectionPool();
	}

	@Override
	public RangeReadRuntime<DataItem> rangeReadRuntime() {
		return runtime;
	}

	@Override
	protected boolean prepare(RangeReadOperation<DataItem> op) {
		op.reset();
		return true;
	}

	@Override
	protected boolean supportsDirectDispatch() {
		return false;
	}

	@Override
	protected boolean successfulSubmitStartsTransport(RangeReadOperation<DataItem> op) {
		return false;
	}

	@Override
	protected boolean offerIncomingOperationLocked(RangeReadOperation<DataItem> op,
					BlockingQueue<RangeReadOperation<DataItem>> queue) {
		return ranges.offer(op, queue);
	}

	@Override
	protected boolean claimDispatchOwnership(RangeReadOperation<DataItem> op) {
		return ranges.capture(op) != null;
	}

	@Override
	protected boolean recoverQueuedOperation(RangeReadOperation<DataItem> op) {
		return ranges.recover(op);
	}

	@Override
	protected List<RangeReadOperation<DataItem>> recoverAdditionalQueuedOperations() {
		return ranges.recoveryCandidates();
	}

	@Override
	protected void appendHandlers(Channel channel) {
		super.appendHandlers(channel);
		final var pipeline = channel.pipeline();
		pipeline.replace(HttpClientCodec.class, "range-response-decoder", new RangeReadResponseDecoder(maxChunkSize));
		pipeline.addAfter("range-response-decoder", "range-request-encoder", new HttpRequestEncoder());
		pipeline.replace(S3ResponseHandler.class, "range-response-handler",
						new RangeReadResponseHandler((attempt, reusable) -> finish(attempt, channel, reusable)));
	}

	/** Build a fresh signed request from the retained span without modifying item size/name/version. */
	FullHttpRequest rangeRequest(RangeReadOperation<DataItem> op, RangeReadAttempt attempt, String node)
					throws URISyntaxException {
		final String uri = dataUriPath(op.item(), op.srcPath(), op.dstPath(), OpType.READ);
		final var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
		try {
			final var headers = request.headers();
			applyDynamicHeaders(headers);
			applySharedHeaders(headers);
			headers.set(HttpHeaderNames.HOST, node);
			headers.set(HttpHeaderNames.CONTENT_LENGTH, 0);
			headers.set(HttpHeaderNames.RANGE, attempt.range().requestHeader());
			applyAuthHeaders(headers, HttpMethod.GET, uri, op.credential());
			return request;
		} catch (Throwable failure) {
			request.release();
			throw failure;
		}
	}

	@Override
	protected boolean submit(RangeReadOperation<DataItem> op) {
		if (!isStarted() || !beginDispatch(op) || !concurrencyThrottle.tryAcquire())
			return false;
		final var attempt = ranges.capture(op);
		if (attempt == null) {
			concurrencyThrottle.release();
			return true;
		}
		Channel channel = null;
		try {
			while (isAdmissionOpen()) {
				channel = rangePool.lease();
				if (channel.isActive())
					break;
				final var stale = channel;
				channel = null;
				stale.close();
				rangePool.release(stale);
			}
			if (channel == null) {
				concurrencyThrottle.release();
				ranges.recover(op);
				return true;
			}
			final var flight = new Flight(op, op.circulation(), attempt, channel);
			if (flights.putIfAbsent(attempt, flight) != null) {
				channel.close();
				rangePool.release(channel);
				concurrencyThrottle.release();
				return true;
			}
			channel.eventLoop().execute(() -> send(flight));
		} catch (Exception failure) {
			attempt.transportFailure(failure instanceof ConnectException ? Operation.Status.FAIL_IO : Operation.Status.FAIL_UNKNOWN);
			if (channel == null) {
				concurrencyThrottle.release();
				completeAttempt(op, op.circulation(), attempt);
			} else {
				finish(attempt, channel, false);
			}
		}
		return true;
	}

	@Override
	protected int submit(List<RangeReadOperation<DataItem>> ops, int from, int to) {
		int i = from;
		while (i < to && submit(ops.get(i)))
			i++;
		return i - from;
	}

	private void send(Flight flight) {
		final var channel = flight.channel();
		final var attempt = flight.attempt();
		FullHttpRequest request = null;
		try {
			final var handler = channel.pipeline().get(RangeReadResponseHandler.class);
			request = rangeRequest(flight.operation(), attempt, channel.attr(NonBlockingConnPool.ATTR_KEY_NODE).get());
			final var outbound = request;
			final boolean sent = withDispatchAdmission(() -> {
				handler.bind(attempt);
				if (!ranges.requestHandoff(flight.operation(), attempt))
					return false;
				flight.operation().nodeAddr(channel.attr(NonBlockingConnPool.ATTR_KEY_NODE).get());
				channel.writeAndFlush(outbound).addListener(future -> {
					if (future.isSuccess())
						attempt.requestComplete();
					else
						handler.fail(attempt, Operation.Status.FAIL_IO);
				});
				return true;
			});
			if (sent)
				request = null;
			else {
				// Admission was fenced before transport. Preserve unattempted/retry settlement.
				attempt.cancelBeforeHandoff();
				finish(attempt, channel, false);
				ranges.recover(flight.operation());
			}
		} catch (Exception failure) {
			attempt.transportFailure(Operation.Status.FAIL_UNKNOWN);
			finish(attempt, channel, false);
		} finally {
			ReferenceCountUtil.release(request);
		}
	}

	private void finish(RangeReadAttempt attempt, Channel channel, boolean reusable) {
		final var flight = flights.get(attempt);
		if (flight == null || flight.channel() != channel || !flights.remove(attempt, flight))
			return;
		try {
			if (!reusable)
				channel.close();
			rangePool.release(channel);
		} catch (RuntimeException failure) {
			channel.close();
			recordTerminalFailure(new IntegrityTerminalException(IntegrityTerminalException.Category.EXECUTION,
							"Range transport cleanup failed", failure));
		} finally {
			concurrencyThrottle.release();
			signalDispatchCapacityAvailable();
			if (attempt.outcome() != null)
				completeAttempt(flight.operation(), flight.circulation(), attempt);
		}
	}

	private void completeAttempt(RangeReadOperation<DataItem> op, RangeReadCirculation circulation, RangeReadAttempt attempt) {
		if (ranges.completed(op, circulation, attempt))
			recordRetainedAttemptCompletion(false);
	}

	int activeRangeTransports() {
		return flights.size();
	}
}
