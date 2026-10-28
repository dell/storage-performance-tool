package com.dell.spt.storage.driver.coop.netty.nic;

import com.dell.spt.storage.driver.coop.netty.nic.NicPathPool.NicPath;
import com.dell.spt.storage.driver.coop.netty.nic.NicPathPool.ProbeResult;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import java.net.ConnectException;
import java.net.Inet4Address;
import java.net.NoRouteToHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/**
 * The connection pool of a driver with a NIC set: one {@link NicPathPool} per chosen NIC. It is
 * created empty while the driver is constructed, when the NICs are not resolved yet, and opened
 * once at start.
 *
 * <p>A request attempt names its NIC, which the dispatch task took from the pool's
 * {@link #admission()} (item 8b), and stays on it. The synchronous {@link #lease()} instead takes
 * the next NIC in rotation that has an idle connection or a free slot, and the first NIC of that
 * rotation when every NIC is at its limit.
 *
 * <p>The synchronous {@link NonBlockingConnPool} methods serve inherited release and close paths;
 * {@link #lease()} waits on the calling thread, never on an event loop of this pool.
 */
public final class NicSetConnectionPool implements NonBlockingConnPool {

	/** Receives the outcome of one acquisition exactly once: an active channel, or the failure. */
	@FunctionalInterface
	public interface Acquisition {
		void complete(Channel channel, Throwable failure);
	}

	/** Whether the host's routing table has a route through {@code nic} that covers {@code destination}. */
	@FunctionalInterface
	public interface RouteLookup {
		boolean routes(String nic, Inet4Address destination);
	}

	/** A NIC of the layout with the event-loop group its connections run on. */
	public record OpenNic(NicSetLayout.Nic nic, EventLoopGroup group) {}

	/** One Path's state, for the step log and tests. */
	public record PathState(String nic, String endpoint, long connectsStarted, boolean inBackoff) {}

	/** One NIC's connections, for the step log and tests. */
	public record NicState(String nic, int openLimit, int open, int idle, int waiting, List<PathState> paths) {}

	private static final long PROBE_MARGIN_MILLIS = 2_000;

	private final Bootstrap template;
	private final ChannelPoolHandler channelHandler;
	private final LongSupplier nanoClock;
	private final AtomicInteger next = new AtomicInteger();
	private volatile List<NicPathPool> pools = List.of();
	private volatile NicAdmission admission = NicAdmission.none();
	private volatile int connectTimeoutMillis;
	private volatile boolean closed;

	/**
	 * @param template the driver's bootstrap as it is at construction, whose options every Path
	 *     connection gets, as the 5.16 pool's per-node clones do
	 */
	public NicSetConnectionPool(final Bootstrap template, final ChannelPoolHandler channelHandler) {
		this(template, channelHandler, System::nanoTime);
	}

	NicSetConnectionPool(final Bootstrap template, final ChannelPoolHandler channelHandler, final LongSupplier nanoClock) {
		this.template = template;
		this.channelHandler = channelHandler;
		this.nanoClock = nanoClock;
	}

	/**
	 * Builds one Path Pool per NIC, in layout order, each limited per {@link NicSetSettings#openLimit}
	 * and binding its sockets with {@code binding}, and the NICs' admission for
	 * {@code concurrencyLimit}. Once only.
	 */
	public synchronized void open(final List<OpenNic> nics, final NicBinding binding, final int connectTimeoutMillis,
					final int concurrencyLimit) {
		if (!pools.isEmpty()) {
			throw new IllegalStateException("The NIC-set pool is already open");
		}
		if (closed) {
			throw new IllegalStateException("The NIC-set pool is closed");
		}
		final var built = new ArrayList<NicPathPool>(nics.size());
		for (var i = 0; i < nics.size(); i++) {
			final var nic = nics.get(i).nic();
			built.add(new NicPathPool(i, nic.name(), nic.address(), nics.get(i).group(), template, channelHandler, binding,
							connectTimeoutMillis, NicSetSettings.openLimit(concurrencyLimit, nics.size(), i), nic.paths(),
							nanoClock));
		}
		this.connectTimeoutMillis = connectTimeoutMillis;
		this.admission = NicAdmission.of(concurrencyLimit, nics.size());
		this.pools = List.copyOf(built);
	}

	/** The NICs' admission; it has no NIC until {@link #open}. */
	public NicAdmission admission() {
		return admission;
	}

	/** The number of NICs; 0 until {@link #open}. */
	public int nicCount() {
		return pools.size();
	}

	/** The index of the NIC whose Path Pool opened {@code channel}, or -1 for a channel of no Path Pool. */
	public static int nicIndex(final Channel channel) {
		final var path = channel.attr(NicPathPool.ATTR_KEY_PATH).get();
		return path == null ? -1 : path.pool().index();
	}

	/**
	 * Opens and closes one TCP connection on every Path at once and returns one message per Path
	 * that failed, naming the NIC and the endpoint (item 9a). Empty when every Path connected.
	 */
	public List<String> probe(final int timeoutMillis, final RouteLookup routes) throws InterruptedException {
		final var results = new ArrayList<CompletableFuture<ProbeResult>>();
		for (final var pool : pools) {
			results.addAll(pool.probe(timeoutMillis));
		}
		try {
			CompletableFuture.allOf(results.toArray(CompletableFuture[]::new))
							.get(timeoutMillis + PROBE_MARGIN_MILLIS, TimeUnit.MILLISECONDS);
		} catch (final ExecutionException | TimeoutException e) {
			// Each result completes on its own; unfinished ones are reported below.
		}
		final var failures = new ArrayList<String>();
		var index = 0;
		for (final var pool : pools) {
			for (final var path : pool.paths()) {
				final var result = results.get(index++).getNow(null);
				if (result == null) {
					failures.add(describe(path, "no answer within " + timeoutMillis + " ms"));
				} else if (result.failure() != null) {
					failures.add(describe(path, reason(path, result.failure(), timeoutMillis, routes)));
				}
			}
		}
		return failures;
	}

	/**
	 * Pre-opens each NIC's connections per {@link NicSetSettings#preOpenCount} and waits for them at
	 * most {@code waitMillis}; connects still pending then complete on their own.
	 */
	public void preOpen(final int concurrencyLimit, final long waitMillis) throws InterruptedException {
		final var current = pools;
		final var opening = new ArrayList<CompletableFuture<Void>>(current.size());
		for (var i = 0; i < current.size(); i++) {
			final var pool = current.get(i);
			opening.add(pool.preOpen(NicSetSettings.preOpenCount(concurrencyLimit, current.size(), i, pool.paths().size())));
		}
		try {
			CompletableFuture.allOf(opening.toArray(CompletableFuture[]::new)).get(waitMillis, TimeUnit.MILLISECONDS);
		} catch (final ExecutionException | TimeoutException e) {
			// Pre-open failures warn per Path and start its back-off; they never fail the step.
		}
	}

	/**
	 * Reports an active channel on one of NIC {@code nic}'s Paths, or the failure, to
	 * {@code callback} exactly once. A channel is reported on its own event loop.
	 */
	public void acquire(final int nic, final Acquisition callback) {
		final var current = pools;
		if (closed || current.isEmpty()) {
			callback.complete(null, new ConnectException(closed ? "The NIC-set pool is closed" : "The NIC-set pool is not open"));
			return;
		}
		current.get(nic).acquire(callback);
	}

	/**
	 * Takes a NIC for the acquisition and reports an active channel on one of its Paths, or the
	 * failure, to {@code callback} exactly once. A channel is reported on its own event loop.
	 */
	public void acquire(final Acquisition callback) {
		final var current = pools;
		if (closed || current.isEmpty()) {
			callback.complete(null, new ConnectException(closed ? "The NIC-set pool is closed" : "The NIC-set pool is not open"));
			return;
		}
		final var size = current.size();
		final var first = Math.floorMod(next.getAndIncrement(), size);
		for (var i = 0; i < size; i++) {
			final var pool = current.get((first + i) % size);
			if (pool.hasCapacity()) {
				pool.acquire(callback);
				return;
			}
		}
		current.get(first).acquire(callback);
	}

	/**
	 * Closes admission on every NIC: no connect starts any more, and every waiting or connecting
	 * acquisition fails at once, its pending connect cancelled.
	 */
	public void closeAdmission() {
		for (final var pool : pools) {
			pool.closeAdmission();
		}
	}

	@Override
	public Channel lease() throws ConnectException {
		final var result = new CompletableFuture<Channel>();
		acquire((channel, failure) -> {
			if (failure != null) {
				result.completeExceptionally(failure);
			} else if (!result.complete(channel)) {
				release(channel);
			}
		});
		final var waitMillis = (long) Math.max(connectTimeoutMillis, NicSetSettings.DEFAULT_CONNECT_TIMEOUT_MILLIS) + PROBE_MARGIN_MILLIS;
		try {
			return result.get(waitMillis, TimeUnit.MILLISECONDS);
		} catch (final TimeoutException e) {
			if (!result.cancel(false) && result.isDone() && !result.isCompletedExceptionally()) {
				release(result.getNow(null));
			}
			throw new ConnectException("No NIC-set connection within " + waitMillis + " ms");
		} catch (final ExecutionException e) {
			final var failure = new ConnectException(String.valueOf(e.getCause().getMessage()));
			failure.initCause(e.getCause());
			throw failure;
		} catch (final InterruptedException e) {
			result.cancel(false);
			Thread.currentThread().interrupt();
			throw new ConnectException("Interrupted while waiting for a NIC-set connection");
		}
	}

	@Override
	public int lease(final List<Channel> channels, final int count) throws ConnectException {
		for (var i = 0; i < count; i++) {
			channels.add(lease());
		}
		return count;
	}

	@Override
	public void release(final Channel channel) {
		final var path = channel.attr(NicPathPool.ATTR_KEY_PATH).get();
		if (path == null) {
			final var unusedClose = channel.close();
			return;
		}
		path.pool().release(channel);
	}

	@Override
	public void release(final List<Channel> channels) {
		channels.forEach(this::release);
	}

	/** Pre-opens for a positive concurrency limit, as the base driver's start asks. */
	@Override
	public void preConnect(final int count) throws InterruptedException {
		preOpen(count, NicSetSettings.MAX_PROBE_TIMEOUT_MILLIS);
	}

	/** Settles everything outstanding and closes every connection. Idempotent; safe after the loops terminated. */
	@Override
	public void close() {
		closed = true;
		for (final var pool : pools) {
			pool.close();
		}
	}

	/** The NICs in layout order with their connections and Paths. */
	public List<NicState> state() {
		final var states = new ArrayList<NicState>();
		for (final var pool : pools) {
			final var paths = pool.paths().stream()
							.map(path -> new PathState(pool.nic(), path.endpoint(), path.connectsStarted(), path.inBackoff()))
							.toList();
			states.add(new NicState(pool.nic(), pool.openLimit(), pool.openCount(), pool.idleCount(), pool.waiterCount(), paths));
		}
		return states;
	}

	/** Acquisitions on every NIC that have not reported yet. */
	public int pendingCount() {
		return pools.stream().mapToInt(NicPathPool::pendingCount).sum();
	}

	List<NicPathPool> pools() {
		return pools;
	}

	private static String describe(final NicPath path, final String reason) {
		return "NIC " + path.nic() + " (" + path.pool().address().getHostAddress() + ") cannot connect to endpoint "
						+ path.endpoint() + " (" + path.destination().getAddress().getHostAddress() + ":"
						+ path.destination().getPort() + "): " + reason;
	}

	/** The probe failure's reason; an unreachable destination is explained from the routing table. */
	static String reason(final NicPath path, final Throwable failure, final int timeoutMillis, final RouteLookup routes) {
		if (failure instanceof ConnectTimeoutException) {
			return "no answer within " + timeoutMillis + " ms";
		}
		final var message = String.valueOf(failure.getMessage()).toLowerCase(Locale.ROOT);
		if (failure instanceof NoRouteToHostException || message.contains("no route to host") || message.contains("unreachable")) {
			final var destination = (Inet4Address) path.destination().getAddress();
			return routes.routes(path.nic(), destination)
							? "a route through " + path.nic() + " covers " + destination.getHostAddress()
											+ ", but no host answered there (no route to host)"
							: "no connected subnet and no gateway route through " + path.nic() + " reaches "
											+ destination.getHostAddress();
		}
		if (message.contains("refused")) {
			return "connection refused";
		}
		if (message.contains("timed out")) {
			return "no answer within " + timeoutMillis + " ms";
		}
		return String.valueOf(failure.getMessage());
	}
}
