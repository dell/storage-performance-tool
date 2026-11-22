package com.dell.spt.storage.driver.coop.netty.nic;

import static com.github.akurilov.netty.connection.pool.NonBlockingConnPool.ATTR_KEY_NODE;

import com.dell.spt.base.logging.Loggers;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetConnectionPool.Acquisition;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.util.AttributeKey;
import java.net.ConnectException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * One NIC's Path Pool (item 8a). Its connections run on the NIC's own event-loop group, bind to the
 * NIC (item 9) and connect asynchronously to each Path's frozen address (item 7a). Idle connections
 * are kept per Path; the channel's node attribute is the configured endpoint string, so the
 * operation's node address and the Host header stay as in 5.16.
 *
 * <p>An acquisition takes an idle connection on any of the NIC's Paths. When none is idle and the
 * NIC is below its open-connection limit, it opens one on the NIC's next Path in rotation; at the
 * limit it waits for a released connection or a freed slot. A connect never blocks a thread. A Path
 * in back-off fails an acquisition that needs a new connection on it at once, and rotation does
 * not skip it (D21).
 *
 * <p>Each acquisition reports exactly once: a channel on the channel's own event loop, a failure on
 * the thread that observes it. {@link #closeAdmission()} and {@link #close()} settle every
 * outstanding acquisition and cancel every pending connect, also after the loops have terminated.
 */
final class NicPathPool implements NicAdmission.Gate {

	static final AttributeKey<NicPath> ATTR_KEY_PATH = AttributeKey.valueOf("nicPath");
	private static final AttributeKey<Long> ATTR_KEY_FAILURE_EPOCH = AttributeKey.valueOf("nicFailureEpoch");

	private final int index;
	private final String nic;
	private final Inet4Address address;
	private final EventLoopGroup group;
	private final Bootstrap template;
	private final ChannelPoolHandler channelHandler;
	private final NicBinding binding;
	private final int connectTimeoutMillis;
	private final int recoveryTimeoutMillis;
	private final Runnable recheck;
	private final int openLimit;
	private final List<NicPath> paths;

	// Guarded by this.
	private final Deque<Pending> waiters = new ArrayDeque<>();
	private int open;
	private int idle;
	private int nextPath;
	private int idleCursor;
	private boolean admissionClosed;
	private boolean closed;
	// From a failure with no response until an attempt gets one (D87, ticket 06): only connections
	// opened after the last failure are pooled, and a trial waits for one.
	private boolean recovering;
	private long failureEpoch;
	private boolean recoveryConnecting;
	private int recoveryCursor;

	private final Set<Pending> pending = ConcurrentHashMap.newKeySet();
	private final Set<Channel> openChannels = ConcurrentHashMap.newKeySet();

	/** One Path of this NIC: the configured endpoint string and its frozen destination. */
	final class NicPath {

		private final int index;
		private final String endpoint;
		private final InetSocketAddress destination;
		private final PathBackoff backoff;
		private final Deque<Channel> idleChannels = new ArrayDeque<>();
		private final AtomicLong connectsStarted = new AtomicLong();
		private final AtomicBoolean preOpenWarned = new AtomicBoolean();

		private NicPath(final int index, final String endpoint, final InetSocketAddress destination,
						final LongSupplier nanoClock) {
			this.index = index;
			this.endpoint = endpoint;
			this.destination = destination;
			this.backoff = new PathBackoff(nanoClock);
		}

		String nic() {
			return nic;
		}

		NicPathPool pool() {
			return NicPathPool.this;
		}

		String endpoint() {
			return endpoint;
		}

		InetSocketAddress destination() {
			return destination;
		}

		int index() {
			return index;
		}

		boolean inBackoff() {
			return backoff.inBackoff();
		}

		/** Connects started on this Path by acquisitions and pre-opening; probes and recovery connects not included. */
		long connectsStarted() {
			return connectsStarted.get();
		}

		@Override
		public String toString() {
			return nic + " -> " + endpoint;
		}
	}

	/** The outcome of probing one Path: {@code failure} is null when it connected. */
	record ProbeResult(NicPath path, Throwable failure) {}

	/**
	 * One outstanding acquisition. It reports exactly once: a failure on the thread that observes
	 * it, or a channel on that channel's own event loop. A channel handed to its loop stays tracked,
	 * and closing can still cancel it, until the loop claims delivery; a cancelled hand-off returns
	 * the channel to the pool when it runs.
	 */
	private final class Pending {

		private static final int WAITING = 0;
		private static final int QUEUED = 1;
		private static final int DONE = 2;
		// Settled by a failure, so a channel still connecting for it has no owner.
		private static final int FAILED = 3;

		private final Acquisition callback;
		private final AtomicInteger state = new AtomicInteger(WAITING);
		// The Path chosen for a new connection, set before the connect or its rejection; -1 before.
		private volatile int path = -1;

		private Pending(final Acquisition callback) {
			this.callback = callback;
		}

		void chose(final NicPath chosen) {
			path = chosen.index;
		}

		boolean fail(final Throwable failure) {
			while (true) {
				final var current = state.get();
				if (current >= DONE) {
					return false;
				}
				if (state.compareAndSet(current, FAILED)) {
					pending.remove(this);
					callback.failed(path, failure);
					return true;
				}
			}
		}

		boolean failed() {
			return state.get() == FAILED;
		}

		/** Hands an active channel over on its own loop; false if already settled. */
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
						release(channel);
					}
				});
			} catch (final RejectedExecutionException e) {
				final var unusedClose = channel.close();
				fail(closedFailure());
			}
			return true;
		}
	}

	/** @param index the NIC's index in the layout's NIC-set order */
	NicPathPool(final int index, final String nic, final Inet4Address address, final EventLoopGroup group,
					final Bootstrap template, final ChannelPoolHandler channelHandler, final NicBinding binding,
					final int connectTimeoutMillis, final int openLimit, final List<NicSetLayout.PathSpec> pathSpecs,
					final LongSupplier nanoClock) {
		this(index, nic, address, group, template, channelHandler, binding, connectTimeoutMillis, connectTimeoutMillis,
						null, openLimit, pathSpecs, nanoClock);
	}

	/**
	 * @param recoveryTimeoutMillis the connect timeout of a failing NIC's recovery connect
	 * @param recheck wakes the dispatch task when a recovery connect lands, or {@code null}
	 */
	NicPathPool(final int index, final String nic, final Inet4Address address, final EventLoopGroup group,
					final Bootstrap template, final ChannelPoolHandler channelHandler, final NicBinding binding,
					final int connectTimeoutMillis, final int recoveryTimeoutMillis, final Runnable recheck,
					final int openLimit, final List<NicSetLayout.PathSpec> pathSpecs, final LongSupplier nanoClock) {
		if (pathSpecs.isEmpty()) {
			throw new IllegalArgumentException("NIC " + nic + " has no Path");
		}
		this.index = index;
		this.nic = nic;
		this.address = address;
		this.group = group;
		this.template = template;
		this.channelHandler = channelHandler;
		this.binding = binding;
		this.connectTimeoutMillis = connectTimeoutMillis;
		this.recoveryTimeoutMillis = recoveryTimeoutMillis;
		this.recheck = recheck;
		this.openLimit = openLimit;
		final var built = new ArrayList<NicPath>(pathSpecs.size());
		for (final var spec : pathSpecs) {
			built.add(new NicPath(built.size(), spec.endpoint(), spec.destination(), nanoClock));
		}
		this.paths = List.copyOf(built);
	}

	/** The NIC's index in the layout's NIC-set order. */
	int index() {
		return index;
	}

	String nic() {
		return nic;
	}

	Inet4Address address() {
		return address;
	}

	EventLoopGroup group() {
		return group;
	}

	List<NicPath> paths() {
		return paths;
	}

	/** The open-connection limit, or 0 for none. */
	int openLimit() {
		return openLimit;
	}

	/**
	 * Reports an active channel on one of this NIC's Paths, or the failure, to {@code callback}
	 * exactly once.
	 */
	void acquire(final Acquisition callback) {
		final var attempt = new Pending(callback);
		pending.add(attempt);
		Channel reused = null;
		NicPath connectPath = null;
		long epoch = PathBackoff.REJECTED;
		long failures = 0;
		int timeoutMillis = connectTimeoutMillis;
		Throwable failure = null;
		synchronized (this) {
			failures = failureEpoch;
			// A recovering NIC's connect holds its operation no longer than a recovery connect would.
			if (recovering) {
				timeoutMillis = recoveryTimeoutMillis;
			}
			if (admissionClosed) {
				failure = closedFailure();
			} else if ((reused = pollIdle()) == null) {
				if (openLimit > 0 && open >= openLimit) {
					waiters.addLast(attempt);
					return;
				}
				connectPath = nextConnectPath();
				attempt.chose(connectPath);
				epoch = connectPath.backoff.tryConnect();
				if (epoch == PathBackoff.REJECTED) {
					failure = new PathBackoffException(connectPath);
				} else {
					open++;
				}
			} else {
				// The reused connection's Path, published under the monitor that selected it, so an
				// attempt that closing settles before its hand-off counts on that Path.
				attempt.chose(reused.attr(ATTR_KEY_PATH).get());
			}
		}
		if (failure != null) {
			attempt.fail(failure);
		} else if (reused != null) {
			if (!attempt.deliver(reused)) {
				release(reused);
			}
		} else {
			connect(connectPath, epoch, attempt, timeoutMillis, failures, true);
		}
	}

	/**
	 * Whether an acquisition now would not fail at once: admission is open, and it would take an
	 * idle connection or connect on the next Path in rotation, which is not in back-off or may start
	 * its trial. It may still wait for a slot. Asked by the admission only of a failing NIC (D87).
	 *
	 * <p>After a failure with no response, until an answer, only an idle connection opened since that
	 * failure serves. Without one this starts a recovery connect, at most one at a time and only on a
	 * Path not in back-off, that holds no operation, and answers no: a silent NIC then holds no
	 * operation for its timeout (ticket 06).
	 */
	@Override
	public boolean servesNow() {
		final NicPath path;
		final long epoch;
		final long failures;
		final Pending recovery;
		final List<Channel> inactive = new ArrayList<>(0);
		synchronized (this) {
			if (admissionClosed) {
				return false;
			}
			if (!recovering) {
				return idle > 0 || !paths.get(nextPath).backoff.rejectsNow();
			}
			// Every idle connection was opened after the last failure (release keeps no older one); one
			// that closed meanwhile does not serve.
			pruneInactiveIdle(inactive);
			if (idle > 0) {
				return true;
			}
			if (recoveryConnecting || (openLimit > 0 && open >= openLimit)) {
				path = null;
				epoch = PathBackoff.REJECTED;
			} else {
				path = nextRecoveryPath();
				epoch = path == null ? PathBackoff.REJECTED : path.backoff.tryConnect();
			}
			if (epoch == PathBackoff.REJECTED) {
				recovery = null;
			} else {
				recoveryConnecting = true;
				open++;
				recovery = recoveryAttempt();
				// Tracked before the monitor is left, so a close that follows cancels it.
				pending.add(recovery);
			}
			failures = failureEpoch;
		}
		for (final var channel : inactive) {
			final var unusedClose = channel.close();
		}
		if (recovery != null) {
			connect(path, epoch, recovery, recoveryTimeoutMillis, failures, false);
		}
		return false;
	}

	/**
	 * The NIC's attempt ended with no response. Its idle connections, which may be dead without
	 * knowing it, are closed, and from now until an answer only connections opened after this
	 * failure are pooled.
	 */
	@Override
	public void failedWithoutAnswer(final Runnable markFailing) {
		final List<Channel> idleNow;
		synchronized (this) {
			recovering = true;
			failureEpoch++;
			markFailing.run();
			if (idle == 0) {
				return;
			}
			idleNow = new ArrayList<>(idle);
			for (final var path : paths) {
				idleNow.addAll(path.idleChannels);
				path.idleChannels.clear();
			}
			idle = 0;
		}
		for (final var channel : idleNow) {
			final var unusedClose = channel.close();
		}
	}

	/** An attempt on the NIC got a response: its connections are pooled as before. */
	@Override
	public synchronized void answered(final Runnable markAnswered) {
		recovering = false;
		markAnswered.run();
	}

	/** Drops idle connections that closed while pooled, to close outside the monitor. Under the monitor. */
	private void pruneInactiveIdle(final List<Channel> inactive) {
		if (idle == 0) {
			return;
		}
		for (final var path : paths) {
			for (final var channels = path.idleChannels.iterator(); channels.hasNext();) {
				final var channel = channels.next();
				if (!channel.isActive()) {
					channels.remove();
					idle--;
					inactive.add(channel);
				}
			}
		}
	}

	/** The next Path for a recovery connect that is not in back-off, or {@code null}. Under the monitor. */
	private NicPath nextRecoveryPath() {
		for (var i = 0; i < paths.size(); i++) {
			final var path = paths.get((recoveryCursor + i) % paths.size());
			if (!path.backoff.rejectsNow()) {
				recoveryCursor = (path.index + 1) % paths.size();
				return path;
			}
		}
		return null;
	}

	/**
	 * A recovery connect's attempt, which no operation waits for. Once it connects the connection is
	 * pooled as idle, so the trial takes it, and the dispatch task is woken (ticket 06).
	 */
	private Pending recoveryAttempt() {
		return new Pending((channel, failure) -> {
			try {
				if (failure == null) {
					release(channel);
				}
			} finally {
				synchronized (this) {
					recoveryConnecting = false;
				}
			}
			if (failure == null && recheck != null) {
				recheck.run();
			}
		});
	}

	/** Whether an acquisition now would not wait: an idle connection or a free slot. */
	synchronized boolean hasCapacity() {
		return idle > 0 || openLimit <= 0 || open < openLimit;
	}

	/**
	 * Returns a connection: to the oldest waiting acquisition, or to its Path's idle connections.
	 * An inactive connection, or any connection after close, is closed.
	 */
	void release(final Channel channel) {
		final var path = channel.attr(ATTR_KEY_PATH).get();
		if (path == null || !channel.isActive()) {
			final var unusedClose = channel.close();
			return;
		}
		while (true) {
			final Pending waiter;
			synchronized (this) {
				if (closed || (recovering && openedBeforeLastFailure(channel))) {
					waiter = null;
				} else {
					waiter = waiters.pollFirst();
					if (waiter == null) {
						path.idleChannels.offerLast(channel);
						idle++;
						return;
					}
					// The waiter takes this connection's Path while the monitor that paired them is held.
					waiter.chose(path);
				}
			}
			if (waiter == null) {
				final var unusedClose = channel.close();
				return;
			}
			if (waiter.deliver(channel)) {
				return;
			}
			// That waiter was settled meanwhile; offer the channel to the next one.
		}
	}

	/**
	 * Opens {@code count} connections spread evenly over the Paths in rotation, starting at the
	 * first Path. Returns when every connect completed. A failure warns once per Path and starts its
	 * back-off; it never fails the step, since the probe already proved the route.
	 */
	CompletableFuture<Void> preOpen(final int count) {
		final var connects = new ArrayList<CompletableFuture<Void>>(count);
		for (var i = 0; i < count; i++) {
			final var path = paths.get(i % paths.size());
			final long epoch;
			final long failures;
			synchronized (this) {
				if (admissionClosed || (openLimit > 0 && open >= openLimit)) {
					break;
				}
				failures = failureEpoch;
				epoch = path.backoff.tryConnect();
				if (epoch == PathBackoff.REJECTED) {
					continue;
				}
				open++;
			}
			final var done = new CompletableFuture<Void>();
			connects.add(done);
			final var preOpening = new Pending((channel, failure) -> {
				if (failure == null) {
					release(channel);
				} else if (!isAdmissionClosed() && path.preOpenWarned.compareAndSet(false, true)) {
					Loggers.MSG.warn("NIC {}: failed to pre-open a connection to {}: {}", nic, path.endpoint, failure.toString());
				}
				done.complete(null);
			});
			pending.add(preOpening);
			connect(path, epoch, preOpening, connectTimeoutMillis, failures, true);
		}
		return CompletableFuture.allOf(connects.toArray(CompletableFuture[]::new));
	}

	/**
	 * Opens and closes one TCP connection on every Path at once, through the Path's own binding, with
	 * {@code timeoutMillis} as the connect timeout. Probes do not count toward the limit or back-off.
	 */
	List<CompletableFuture<ProbeResult>> probe(final int timeoutMillis) {
		final var results = new ArrayList<CompletableFuture<ProbeResult>>(paths.size());
		for (final var path : paths) {
			final var result = new CompletableFuture<ProbeResult>();
			results.add(result);
			final ChannelFuture connect;
			try {
				final var probing = template.clone(group.next())
								.handler(new ChannelInboundHandlerAdapter())
								.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMillis);
				binding.apply(probing, nic, address);
				connect = probing.connect(path.destination);
			} catch (final RuntimeException e) {
				result.complete(new ProbeResult(path, e));
				continue;
			}
			connect.addListener(done -> {
				if (done.isSuccess()) {
					final var unusedClose = connect.channel().close();
					result.complete(new ProbeResult(path, null));
				} else {
					result.complete(new ProbeResult(path, done.cause()));
				}
			});
		}
		return results;
	}

	/**
	 * Closes admission: every waiting or connecting acquisition fails, and every pending connect is
	 * cancelled with its channel closed, including one starting as this runs. Idle and leased
	 * connections stay until {@link #close()}.
	 */
	void closeAdmission() {
		final List<Pending> settled;
		synchronized (this) {
			admissionClosed = true;
			settled = new ArrayList<>(waiters);
			waiters.clear();
		}
		settled.forEach(waiter -> waiter.fail(closedFailure()));
		for (final var attempt : new ArrayList<>(pending)) {
			attempt.fail(closedFailure());
		}
		for (final var channel : new ArrayList<>(openChannels)) {
			if (!channel.isActive()) {
				final var unusedClose = channel.close();
			}
		}
	}

	/** Settles everything outstanding and closes every connection. Idempotent; safe after the loops terminated. */
	void close() {
		final List<Channel> idleNow = new ArrayList<>();
		synchronized (this) {
			admissionClosed = true;
			closed = true;
			for (final var path : paths) {
				idleNow.addAll(path.idleChannels);
				path.idleChannels.clear();
			}
			idle = 0;
			waiters.clear();
		}
		for (final var attempt : new ArrayList<>(pending)) {
			attempt.fail(closedFailure());
		}
		for (final var channel : idleNow) {
			final var unusedClose = channel.close();
		}
		for (final var channel : new ArrayList<>(openChannels)) {
			final var unusedClose = channel.close();
		}
		// A terminated loop cannot run close listeners; nothing remains in use after close.
		openChannels.clear();
	}

	synchronized boolean isAdmissionClosed() {
		return admissionClosed;
	}

	/** The index of the Path the next connect takes. */
	synchronized int nextConnectPathIndex() {
		return nextPath;
	}

	/** Connections open, idle or connecting. */
	synchronized int openCount() {
		return open;
	}

	synchronized int idleCount() {
		return idle;
	}

	synchronized int waiterCount() {
		return waiters.size();
	}

	/** Acquisitions that have not reported yet: waiting, connecting or being handed over. */
	int pendingCount() {
		return pending.size();
	}

	private boolean openedBeforeLastFailure(final Channel channel) {
		final var epoch = channel.attr(ATTR_KEY_FAILURE_EPOCH).get();
		return epoch == null || epoch < failureEpoch;
	}

	/**
	 * @param failures the failure epoch read under the monitor that chose to connect
	 * @param counted whether the connect counts in {@link NicPath#connectsStarted()}; a recovery connect does not
	 */
	private void connect(final NicPath path, final long epoch, final Pending attempt, final int timeoutMillis,
					final long failures, final boolean counted) {
		attempt.chose(path);
		final EventLoop loop;
		final ChannelFuture connect;
		try {
			loop = group.next();
			if (loop.isShuttingDown()) {
				throw new RejectedExecutionException("NIC " + nic + " event loops are shutting down");
			}
			final var connecting = template.clone(loop)
							.handler(new ChannelInitializer<>() {
								@Override
								protected void initChannel(final Channel channel) throws Exception {
									channelHandler.channelCreated(channel);
								}
							})
							.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMillis);
			binding.apply(connecting, nic, address);
			if (counted) {
				path.connectsStarted.incrementAndGet();
			}
			connect = connecting.connect(path.destination);
		} catch (final RuntimeException e) {
			// A trial that never connected must not hold the Path in back-off for good.
			path.backoff.abandoned(epoch);
			slotFreed();
			attempt.fail(e);
			return;
		}
		final var channel = connect.channel();
		channel.attr(ATTR_KEY_PATH).set(path);
		channel.attr(ATTR_KEY_NODE).set(path.endpoint);
		channel.attr(ATTR_KEY_FAILURE_EPOCH).set(failures);
		openChannels.add(channel);
		channel.closeFuture().addListener(ignored -> openChannels.remove(channel));
		connect.addListener(done -> {
			if (!done.isSuccess()) {
				final var stopping = isAdmissionClosed();
				if (!stopping && !loop.isShuttingDown()) {
					path.backoff.failed(epoch);
				}
				slotFreed();
				attempt.fail(stopping ? closedFailure() : done.cause());
				return;
			}
			path.backoff.connected();
			// Only established connections free their slot on close; a failed connect freed it above.
			channel.closeFuture().addListener(ignored -> slotFreed());
			if (!attempt.deliver(channel)) {
				release(channel);
			}
		});
		// A close that scanned before this registration failed the attempt and missed the connect.
		if (attempt.failed() && isAdmissionClosed()) {
			final var unusedClose = channel.close();
		}
	}

	/**
	 * A connection closed or a connect failed. A waiting acquisition takes the slot: it opens a
	 * connection on the next Path in rotation, or fails at once when that Path is in back-off and the
	 * next waiter tries.
	 */
	private void slotFreed() {
		while (true) {
			final Pending waiter;
			final NicPath path;
			final long epoch;
			final long failures;
			final int timeoutMillis;
			synchronized (this) {
				open--;
				if (closed || admissionClosed || waiters.isEmpty()) {
					return;
				}
				waiter = waiters.pollFirst();
				path = nextConnectPath();
				waiter.chose(path);
				epoch = path.backoff.tryConnect();
				open++;
				failures = failureEpoch;
				timeoutMillis = recovering ? recoveryTimeoutMillis : connectTimeoutMillis;
			}
			if (epoch != PathBackoff.REJECTED) {
				connect(path, epoch, waiter, timeoutMillis, failures, true);
				return;
			}
			waiter.fail(new PathBackoffException(path));
			// The slot is still free for the next waiter; open-- runs again at the loop's start.
		}
	}

	/** The next Path in rotation. Runs under the monitor; the cursor stays below the Path count, so it cannot wrap. */
	private NicPath nextConnectPath() {
		final var path = paths.get(nextPath);
		nextPath = (nextPath + 1) % paths.size();
		return path;
	}

	private Channel pollIdle() {
		if (idle == 0) {
			return null;
		}
		for (var i = 0; i < paths.size(); i++) {
			final var path = paths.get((idleCursor + i) % paths.size());
			Channel channel;
			while ((channel = path.idleChannels.pollLast()) != null) {
				idle--;
				if (channel.isActive()) {
					idleCursor = path.index + 1;
					return channel;
				}
				final var unusedClose = channel.close();
			}
		}
		return null;
	}

	private ConnectException closedFailure() {
		return new ConnectException("NIC " + nic + ": the connection pool is closed");
	}

	/** An acquisition needed a new connection on a Path in connect back-off. */
	static final class PathBackoffException extends ConnectException {

		private static final long serialVersionUID = 1L;

		PathBackoffException(final NicPath path) {
			super("NIC " + path.nic() + ": the Path to " + path.endpoint() + " is in connect back-off");
		}
	}
}
