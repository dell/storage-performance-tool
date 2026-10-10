package com.dell.spt.storage.driver.coop.netty.endpoint;

import static com.github.akurilov.netty.connection.pool.NonBlockingConnPool.ATTR_KEY_NODE;

import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Connection source for opt-in endpoint selection. Every acquisition takes a new destination from
 * its {@link DestinationSource}. A pooled source reuses an idle connection to exactly that
 * destination, otherwise a connection opens asynchronously; waiting never takes another selection
 * and never falls back to another destination. A non-pooled source never reuses a connection and
 * lets the server close it first after a completed exchange.
 *
 * <p>Each acquisition reports its outcome exactly once through an {@link Acquisition} callback that
 * does not depend on any event loop staying alive: {@link #close()} settles every outstanding
 * acquisition on the closing thread, even when the I/O or resolver loops have already terminated.
 *
 * <p>The pool is created during driver construction and bound to its settings once the driver's
 * own fields exist. The synchronous {@link NonBlockingConnPool} methods serve inherited release
 * and close paths and caller-thread helpers; they wait on the calling thread, never on an event
 * loop of this pool.
 */
public final class SelectingConnectionPool implements NonBlockingConnPool {

	/** Receives the outcome of one acquisition exactly once: an active channel, or the failure. */
	@FunctionalInterface
	public interface Acquisition {
		void complete(Channel channel, Throwable failure);
	}

	private static final AttributeKey<InetSocketAddress> ATTR_KEY_DESTINATION = AttributeKey.valueOf("endpointSelectionDestination");

	private final Bootstrap bootstrap;
	private final ChannelPoolHandler channelHandler;
	private final Set<Channel> openChannels = ConcurrentHashMap.newKeySet();
	private final Set<Pending> pending = ConcurrentHashMap.newKeySet();
	private final Map<InetSocketAddress, Deque<Channel>> idleChannels = new ConcurrentHashMap<>();
	private final AtomicInteger idleCount = new AtomicInteger();
	private volatile Binding binding;
	private volatile boolean closed;

	private record Binding(DestinationSource source, boolean pooled, int idleLimit, int connectTimeoutMillis,
					long setupTimeoutMillis, EndpointSelectionCounters counters) {}

	/**
	 * One outstanding acquisition. It reports exactly once: a failure on the thread that observes it,
	 * or a channel on that channel's own event loop. A channel handed to its loop stays tracked, and
	 * {@link #close()} can still cancel it, until the loop claims delivery; a cancelled hand-off only
	 * closes the channel when it runs.
	 */
	private final class Pending {

		private static final int WAITING = 0;
		private static final int QUEUED = 1;
		private static final int DONE = 2;

		private final Acquisition callback;
		private final AtomicInteger state = new AtomicInteger(WAITING);

		private Pending(final Acquisition callback) {
			this.callback = callback;
		}

		/** Reports a failure, cancelling a queued hand-off if necessary. False if already reported. */
		boolean fail(final Throwable failure) {
			while (true) {
				final var current = state.get();
				if (current == DONE) {
					return false;
				}
				if (state.compareAndSet(current, DONE)) {
					pending.remove(this);
					callback.complete(null, failure);
					return true;
				}
			}
		}

		/** Hands an active channel to the callback on the channel's loop. False if already settled. */
		boolean deliver(final Channel channel) {
			final var loop = channel.eventLoop();
			if (loop.inEventLoop()) {
				if (!state.compareAndSet(WAITING, DONE)) {
					return false;
				}
				pending.remove(this);
				callback.complete(channel, null);
				return true;
			}
			if (!state.compareAndSet(WAITING, QUEUED)) {
				return false;
			}
			try {
				loop.execute(() -> {
					if (state.compareAndSet(QUEUED, DONE)) {
						pending.remove(this);
						callback.complete(channel, null);
					} else {
						final var unusedClose = channel.close();
					}
				});
			} catch (final RejectedExecutionException e) {
				final var unusedClose = channel.close();
				fail(closedFailure());
			}
			return true;
		}
	}

	public SelectingConnectionPool(final Bootstrap bootstrap, final ChannelPoolHandler channelHandler) {
		this.bootstrap = bootstrap;
		this.channelHandler = channelHandler;
	}

	/**
	 * Binds the destination source once.
	 *
	 * @param pooled reuse idle connections per destination; otherwise every connection serves one request
	 * @param idleLimit maximum idle connections kept across all destinations
	 * @param setupTimeoutMillis bound for a synchronous {@link #lease()}, covering selection and connect
	 */
	public void bind(final DestinationSource source, final boolean pooled, final int idleLimit,
					final int connectTimeoutMillis, final long setupTimeoutMillis, final EndpointSelectionCounters counters) {
		if (binding != null) {
			throw new IllegalStateException("Connection pool is already bound");
		}
		binding = new Binding(source, pooled, idleLimit, connectTimeoutMillis, setupTimeoutMillis, counters);
	}

	/**
	 * Selects the next destination and reports an active channel to it, or the selection or connect
	 * failure, to {@code callback} exactly once. A channel is reported on its own event loop; a
	 * failure on the thread that observed it, which is the closing thread for acquisitions still
	 * outstanding at {@link #close()}.
	 */
	public void acquire(final Acquisition callback) {
		acquire(true, callback);
	}

	private void acquire(final boolean allowReuse, final Acquisition callback) {
		final var bound = boundOrFail();
		final var attempt = new Pending(callback);
		pending.add(attempt);
		if (closed) {
			attempt.fail(closedFailure());
			return;
		}
		final Future<InetSocketAddress> selection;
		try {
			selection = bound.source().next();
		} catch (final RuntimeException e) {
			attempt.fail(e);
			return;
		}
		// Count each selection exactly once, whether it is already complete or completes later.
		final var countedNow = selection.isSuccess();
		if (countedNow) {
			bound.counters().selected(selection.getNow());
		}
		if (allowReuse && bound.pooled() && countedNow) {
			final var idle = pollActiveIdle(selection.getNow());
			if (idle != null) {
				bound.counters().connected(true);
				if (!attempt.deliver(idle)) {
					release(idle);
				}
				return;
			}
		}
		// An attempt that close() settled first reaches connect(), which counts it as cancelled.
		selection.addListener((Future<InetSocketAddress> selected) -> {
			if (!selected.isSuccess()) {
				attempt.fail(selected.cause());
				return;
			}
			if (!countedNow) {
				bound.counters().selected(selected.getNow());
			}
			connect(bound, selected.getNow(), attempt);
		});
	}

	/**
	 * Opens a connection for a counted selection and counts its outcome once. A connect that never
	 * starts, or ends, while the pool is closed or the I/O loop is stopping is cancelled, not failed.
	 */
	private void connect(final Binding bound, final InetSocketAddress destination, final Pending attempt) {
		final EventLoop loop = bootstrap.config().group().next();
		if (closed || loop.isShuttingDown()) {
			bound.counters().connectCancelled();
			attempt.fail(closedFailure());
			return;
		}
		final var connecting = bootstrap.clone(loop)
						.handler(new ChannelInitializer<>() {
							@Override
							protected void initChannel(final Channel channel) throws Exception {
								channelHandler.channelCreated(channel);
								if (!bound.pooled()) {
									channel.pipeline().addFirst(new ServerCloseGraceHandler());
								}
							}
						})
						.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, bound.connectTimeoutMillis());
		if (!bound.pooled()) {
			// Per-request connections close gracefully after the response; never with a reset.
			connecting.option(ChannelOption.SO_LINGER, -1);
		}
		final var connect = connecting.connect(destination);
		final var channel = connect.channel();
		channel.attr(ATTR_KEY_DESTINATION).set(destination);
		channel.attr(ATTR_KEY_NODE).set(destination.getAddress().getHostAddress() + ":" + destination.getPort());
		openChannels.add(channel);
		channel.closeFuture().addListener(ignored -> openChannels.remove(channel));
		// If the loop terminates before this listener runs, close() settles the attempt instead.
		connect.addListener(done -> {
			if (!done.isSuccess()) {
				if (closed || loop.isShuttingDown()) {
					bound.counters().connectCancelled();
				} else {
					bound.counters().connectFailed();
				}
				attempt.fail(done.cause());
				return;
			}
			bound.counters().connected(false);
			// Only established connections count as closed.
			channel.closeFuture().addListener(ignored -> bound.counters().closed());
			if (closed) {
				attempt.fail(closedFailure());
				final var unusedClose = channel.close();
			} else if (!attempt.deliver(channel)) {
				final var unusedClose = channel.close();
			}
		});
	}

	/**
	 * Selects a destination and opens a new connection that the caller closes after one exchange. A
	 * per-request connection waits for the server's close, like a released workload connection.
	 */
	public Channel connectUnpooled() throws ConnectException {
		final var channel = await(false);
		if (!boundOrFail().pooled()) {
			channel.attr(ServerCloseGraceHandler.AWAIT_SERVER_CLOSE).set(Boolean.TRUE);
		}
		return channel;
	}

	@Override
	public Channel lease() throws ConnectException {
		return await(true);
	}

	@Override
	public int lease(final List<Channel> channels, final int count) throws ConnectException {
		for (var i = 0; i < count; i++) {
			channels.add(lease());
		}
		return count;
	}

	private Channel await(final boolean allowReuse) throws ConnectException {
		final var bound = boundOrFail();
		final var result = new CompletableFuture<Channel>();
		acquire(allowReuse, (channel, failure) -> {
			if (failure != null) {
				result.completeExceptionally(failure);
			} else if (!result.complete(channel)) {
				// The caller gave up waiting; nobody owns this connection.
				final var unusedClose = channel.close();
			}
		});
		try {
			return result.get(bound.setupTimeoutMillis(), TimeUnit.MILLISECONDS);
		} catch (final TimeoutException e) {
			result.cancel(false);
			throw new ConnectException("Endpoint selection did not produce a connection within "
							+ bound.setupTimeoutMillis() + " ms");
		} catch (final ExecutionException e) {
			final var failure = new ConnectException("Endpoint selection failed: " + e.getCause());
			failure.initCause(e.getCause());
			throw failure;
		} catch (final InterruptedException e) {
			result.cancel(false);
			Thread.currentThread().interrupt();
			throw new ConnectException("Interrupted while waiting for an endpoint selection connection");
		}
	}

	/**
	 * Marks a per-request connection whose response has fully arrived, so that closing it, now or by
	 * a later release, waits for the server's close. No effect on pooled connections or after
	 * {@link #close()}. For protocol handlers that close a completed connection before release.
	 */
	public void awaitServerClose(final Channel channel) {
		final var bound = binding;
		if (bound != null && !bound.pooled() && !closed) {
			channel.attr(ServerCloseGraceHandler.AWAIT_SERVER_CLOSE).set(Boolean.TRUE);
		}
	}

	@Override
	public void release(final Channel channel) {
		final var bound = binding;
		final var destination = channel.attr(ATTR_KEY_DESTINATION).get();
		if (bound != null && !bound.pooled() && !closed && channel.isActive()) {
			awaitServerClose(channel);
			final var unusedClose = channel.close();
			return;
		}
		if (bound == null || !bound.pooled() || closed || destination == null || !channel.isActive()) {
			final var unusedClose = channel.close();
			return;
		}
		if (idleCount.incrementAndGet() > bound.idleLimit()) {
			idleCount.decrementAndGet();
			final var unusedClose = channel.close();
			return;
		}
		idleChannels.computeIfAbsent(destination, ignored -> new ConcurrentLinkedDeque<>()).offerLast(channel);
		if (closed) {
			closeIdle();
		}
	}

	@Override
	public void release(final List<Channel> channels) {
		channels.forEach(this::release);
	}

	/** No pre-connect: every connection is attributable to a selection. */
	@Override
	public void preConnect(final int count) {}

	/**
	 * Settles every outstanding acquisition as failed, closes every connection at once and closes
	 * the destination source. Idempotent; safe after the event loops have terminated.
	 */
	@Override
	public void close() {
		closed = true;
		for (final var attempt : new ArrayList<>(pending)) {
			attempt.fail(closedFailure());
		}
		closeIdle();
		for (final var channel : new ArrayList<>(openChannels)) {
			channel.attr(ServerCloseGraceHandler.AWAIT_SERVER_CLOSE).set(Boolean.FALSE);
			final var unusedClose = channel.close();
		}
		// A terminated loop cannot run close listeners; nothing remains in use after close.
		openChannels.clear();
		final var bound = binding;
		if (bound != null) {
			bound.source().close();
		}
	}

	/** Connections currently open or connecting, idle ones included. */
	public int openChannelCount() {
		return openChannels.size();
	}

	public int idleChannelCount() {
		return idleCount.get();
	}

	/** Acquisitions that have not reported an outcome yet. */
	public int pendingAcquisitionCount() {
		return pending.size();
	}

	private Channel pollActiveIdle(final InetSocketAddress destination) {
		final var idle = idleChannels.get(destination);
		if (idle == null) {
			return null;
		}
		Channel channel;
		while ((channel = idle.pollLast()) != null) {
			idleCount.decrementAndGet();
			if (channel.isActive()) {
				return channel;
			}
			final var unusedClose = channel.close();
		}
		return null;
	}

	private void closeIdle() {
		for (final var idle : idleChannels.values()) {
			Channel channel;
			while ((channel = idle.pollLast()) != null) {
				idleCount.decrementAndGet();
				final var unusedClose = channel.close();
			}
		}
	}

	private static ConnectException closedFailure() {
		return new ConnectException("Endpoint selection pool is closed");
	}

	private Binding boundOrFail() {
		final var bound = binding;
		if (bound == null) {
			throw new IllegalStateException("Endpoint selection pool is used before it is bound");
		}
		return bound;
	}
}
