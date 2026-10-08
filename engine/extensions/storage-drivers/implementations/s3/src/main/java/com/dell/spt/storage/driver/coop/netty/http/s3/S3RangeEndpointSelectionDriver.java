package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.github.akurilov.netty.connection.pool.NonBlockingConnPool.ATTR_KEY_NODE;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.range.RangeReadAttempt;
import com.dell.spt.base.item.op.data.range.RangeReadOperation;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.storage.driver.coop.netty.endpoint.EndpointSelectionConstants;
import com.dell.spt.storage.driver.coop.netty.endpoint.EndpointSelectionCounters;
import com.dell.spt.storage.driver.coop.netty.endpoint.EndpointSelectionSettings;
import com.dell.spt.storage.driver.coop.netty.endpoint.SelectingConnectionPool;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpStatusClass;
import io.netty.handler.codec.http.LastHttpContent;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.file.Path;

/**
 * Partial-object Reads with opt-in endpoint selection. Every range attempt, retries included, takes
 * its destination from the configured mode and connects asynchronously, so the dispatcher never
 * waits on setup. Once connected, the inherited flight, request handoff and settlement apply
 * unchanged, with the Logical Authority as Host, signing authority and TLS SNI. A setup failure
 * settles the attempt as {@code FAIL_IO}, as a failed default-pool lease does, and the existing
 * retry policy applies.
 */
final class S3RangeEndpointSelectionDriver extends S3RangeStorageDriver {

	private final EndpointSelectionSupport selection;
	// Assigned by createRangePool() while the superclass constructor runs; no initializer, so the
	// assignment survives this class's own field initialization.
	private SelectingConnectionPool selectingPool;

	static S3RangeEndpointSelectionDriver create(final String stepId, final DataInput input, final Config storage,
					final int batchSize, final RangeReadPolicy policy, final EndpointSelectionSettings settings,
					final Path resolvConf) throws IllegalConfigurationException, InterruptedException {
		return new S3RangeEndpointSelectionDriver(stepId, input, storage, batchSize, policy,
						EndpointSelectionSupport.create(stepId, settings, resolvConf));
	}

	static S3RangeEndpointSelectionDriver create(final String stepId, final DataInput input, final Config storage,
					final int batchSize, final RangeReadPolicy policy, final EndpointSelectionSettings settings)
					throws IllegalConfigurationException, InterruptedException {
		return create(stepId, input, storage, batchSize, policy, settings, EndpointSelectionConstants.RESOLV_CONF);
	}

	private S3RangeEndpointSelectionDriver(final String stepId, final DataInput input, final Config storage,
					final int batchSize, final RangeReadPolicy policy, final EndpointSelectionSupport selection)
					throws InterruptedException {
		super(stepId, input, storage, batchSize, policy);
		this.selection = selection;
		final var helperAuthority = selection.bind(selectingPool, concurrencyLimit, sharedHeaders);
		if (helperAuthority != null) {
			helperAuthority(helperAuthority);
		}
	}

	@Override
	NonBlockingConnPool createRangePool() {
		selectingPool = new SelectingConnectionPool(bootstrap, this);
		return selectingPool;
	}

	@Override
	protected boolean submit(final RangeReadOperation<DataItem> op) {
		if (!isStarted() || !beginDispatch(op) || !concurrencyThrottle.tryAcquire())
			return false;
		final var attempt = ranges.capture(op);
		if (attempt == null) {
			concurrencyThrottle.release();
			return true;
		}
		try {
			// The pool reports exactly once: a channel on its own event loop, or the failure.
			selectingPool.acquire((channel, failure) -> {
				if (failure == null)
					start(op, attempt, channel);
				else
					failSetup(op, attempt, failure);
			});
		} catch (final RuntimeException e) {
			failSetup(op, attempt, e);
		}
		return true;
	}

	/** Runs on the channel's event loop, as the default route's handoff task does. */
	private void start(final RangeReadOperation<DataItem> op, final RangeReadAttempt attempt, final Channel channel) {
		final var flight = new Flight(op, op.circulation(), attempt, channel);
		if (flights.putIfAbsent(attempt, flight) != null) {
			final var unusedClose = channel.close();
			selectingPool.release(channel);
			concurrencyThrottle.release();
			signalDispatchCapacityAvailable();
			return;
		}
		send(flight, selection.authority(channel.attr(ATTR_KEY_NODE).get()));
	}

	/** Mirrors a failed default-pool lease: the attempt fails with FAIL_IO and its permit is released. */
	private void failSetup(final RangeReadOperation<DataItem> op, final RangeReadAttempt attempt, final Throwable failure) {
		selection.logSetupFailure(failure);
		attempt.transportFailure(Operation.Status.FAIL_IO);
		concurrencyThrottle.release();
		signalDispatchCapacityAvailable();
		completeAttempt(op, op.circulation(), attempt);
	}

	/** Helper requests (bucket checks) also follow the selection. */
	@Override
	protected Channel getUnpooledConnection(final String storageNodeAddr, final int storageNodePort)
					throws ConnectException {
		return selectingPool.connectUnpooled();
	}

	@Override
	protected void appendHandlers(final Channel channel) {
		super.appendHandlers(channel);
		selection.applySni(channel);
		if (selection.perRequestConnections()) {
			channel.pipeline().addAfter("range-response-decoder", "range-response-arrived",
							new ResponseArrivedMarker(selectingPool));
		}
	}

	@Override
	protected void doStart() throws IllegalStateException {
		super.doStart();
		selection.warnIfHostConfiguredDns();
	}

	@Override
	protected void doClose() throws IllegalStateException, IOException {
		try {
			super.doClose();
		} finally {
			selection.logSummary();
		}
	}

	SelectingConnectionPool connectionPool() {
		return selectingPool;
	}

	EndpointSelectionCounters counters() {
		return selection.counters();
	}

	/**
	 * The range response handler closes a non-keep-alive connection itself, before the pool sees
	 * it. Marking a per-request connection once its final response has fully arrived lets that
	 * close, and the release that follows, wait for the server's close.
	 */
	private static final class ResponseArrivedMarker extends ChannelInboundHandlerAdapter {

		private final SelectingConnectionPool pool;
		private boolean finalResponse;

		ResponseArrivedMarker(final SelectingConnectionPool pool) {
			this.pool = pool;
		}

		@Override
		public void channelRead(final ChannelHandlerContext ctx, final Object message) {
			if (message instanceof HttpResponse response) {
				finalResponse = response.status().codeClass() != HttpStatusClass.INFORMATIONAL;
			}
			if (finalResponse && message instanceof LastHttpContent last && last.decoderResult().isSuccess()) {
				pool.awaitServerClose(ctx.channel());
			}
			ctx.fireChannelRead(message);
		}
	}
}
