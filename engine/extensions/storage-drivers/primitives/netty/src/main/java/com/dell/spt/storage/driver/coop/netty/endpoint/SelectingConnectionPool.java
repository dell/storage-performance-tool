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
import io.netty.util.concurrent.Promise;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Connection source for opt-in endpoint selection. Every acquisition takes a new destination from
 * its {@link DestinationSource}. A pooled source reuses an idle connection to exactly that
 * destination, otherwise a connection opens asynchronously; waiting never takes another selection
 * and never falls back to another destination. A non-pooled source closes every connection on
 * release.
 *
 * <p>The pool is created during driver construction and bound to its settings once the driver's
 * own fields exist. The synchronous {@link NonBlockingConnPool} methods serve inherited release
 * and close paths and caller-thread helpers; they wait on the calling thread, never on an event
 * loop of this pool.
 */
public final class SelectingConnectionPool implements NonBlockingConnPool {

	private static final AttributeKey<InetSocketAddress> ATTR_KEY_DESTINATION = AttributeKey.valueOf("endpointSelectionDestination");

	private final Bootstrap bootstrap;
	private final ChannelPoolHandler channelHandler;
	private final Set<Channel> openChannels = ConcurrentHashMap.newKeySet();
	private final Map<InetSocketAddress, Deque<Channel>> idleChannels = new ConcurrentHashMap<>();
	private final AtomicInteger idleCount = new AtomicInteger();
	private volatile Binding binding;
	private volatile boolean closed;

	private record Binding(DestinationSource source, boolean pooled, int idleLimit, int connectTimeoutMillis,
					long setupTimeoutMillis) {}

	public SelectingConnectionPool(final Bootstrap bootstrap, final ChannelPoolHandler channelHandler) {
		this.bootstrap = bootstrap;
		this.channelHandler = channelHandler;
	}

	/**
	 * Binds the destination source once.
	 *
	 * @param pooled reuse idle connections per destination; otherwise close each connection on release
	 * @param idleLimit maximum idle connections kept across all destinations
	 * @param setupTimeoutMillis bound for a synchronous {@link #lease()}, covering selection and connect
	 */
	public void bind(final DestinationSource source, final boolean pooled, final int idleLimit,
					final int connectTimeoutMillis, final long setupTimeoutMillis) {
		if (binding != null) {
			throw new IllegalStateException("Connection pool is already bound");
		}
		binding = new Binding(source, pooled, idleLimit, connectTimeoutMillis, setupTimeoutMillis);
	}

	/**
	 * Selects the next destination and completes with an active channel to it, or fails with the
	 * selection or connect failure. Completion happens on the selected channel's event loop.
	 */
	public Future<Channel> acquire() {
		return acquire(true);
	}

	private Future<Channel> acquire(final boolean allowReuse) {
		final var bound = boundOrFail();
		final EventLoop loop = bootstrap.config().group().next();
		if (closed) {
			return loop.newFailedFuture(new ConnectException("Endpoint selection pool is closed"));
		}
		final var selection = bound.source().next();
		if (allowReuse && bound.pooled() && selection.isSuccess()) {
			final var idle = pollActiveIdle(selection.getNow());
			if (idle != null) {
				// Completing on the channel's own loop lets the caller send without another hand-off.
				return idle.eventLoop().newSucceededFuture(idle);
			}
		}
		final Promise<Channel> result = loop.newPromise();
		selection.addListener((Future<InetSocketAddress> selected) -> {
			if (selected.isSuccess()) {
				connect(bound, loop, selected.getNow(), result);
			} else {
				result.tryFailure(selected.cause());
			}
		});
		return result;
	}

	private void connect(final Binding bound, final EventLoop loop, final InetSocketAddress destination,
					final Promise<Channel> result) {
		if (closed) {
			result.tryFailure(new ConnectException("Endpoint selection pool is closed"));
			return;
		}
		final var connecting = bootstrap.clone(loop)
						.handler(new ChannelInitializer<>() {
							@Override
							protected void initChannel(final Channel channel) throws Exception {
								channelHandler.channelCreated(channel);
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
		connect.addListener(done -> {
			if (!done.isSuccess()) {
				result.tryFailure(done.cause());
			} else if (closed || !result.trySuccess(channel)) {
				final var unusedClose = channel.close();
				result.tryFailure(new ConnectException("Endpoint selection pool is closed"));
			}
		});
	}

	/** Selects a destination and opens a new connection that the caller closes after use. */
	public Channel connectUnpooled() throws ConnectException {
		return await(acquire(false));
	}

	@Override
	public Channel lease() throws ConnectException {
		return await(acquire(true));
	}

	@Override
	public int lease(final List<Channel> channels, final int count) throws ConnectException {
		for (var i = 0; i < count; i++) {
			channels.add(lease());
		}
		return count;
	}

	private Channel await(final Future<Channel> acquisition) throws ConnectException {
		final var bound = boundOrFail();
		if (!acquisition.awaitUninterruptibly(bound.setupTimeoutMillis(), TimeUnit.MILLISECONDS)) {
			acquisition.cancel(false);
			throw new ConnectException("Endpoint selection did not produce a connection within "
							+ bound.setupTimeoutMillis() + " ms");
		}
		if (!acquisition.isSuccess()) {
			final var failure = new ConnectException("Endpoint selection failed: " + acquisition.cause());
			failure.initCause(acquisition.cause());
			throw failure;
		}
		return acquisition.getNow();
	}

	@Override
	public void release(final Channel channel) {
		final var bound = binding;
		final var destination = channel.attr(ATTR_KEY_DESTINATION).get();
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

	/** Closes every connection and the destination source. Idempotent. */
	@Override
	public void close() {
		closed = true;
		closeIdle();
		for (final var channel : new ArrayList<>(openChannels)) {
			final var unusedClose = channel.close();
		}
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

	private Binding boundOrFail() {
		final var bound = binding;
		if (bound == null) {
			throw new IllegalStateException("Endpoint selection pool is used before it is bound");
		}
		return bound;
	}
}
