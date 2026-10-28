package com.dell.spt.storage.driver.coop.netty.nic;

import static com.github.akurilov.netty.connection.pool.NonBlockingConnPool.ATTR_KEY_NODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.storage.driver.coop.netty.nic.NicPathPool.PathBackoffException;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetConnectionPool.OpenNic;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import java.net.ConnectException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Real epoll sockets on loopback. Each NIC is a loopback source address (127.0.0.2, 127.0.0.3)
 * bound per connection; the listeners on 127.0.0.1 accept and hold connections and record their
 * sources. Back-off runs on a test clock.
 */
@Timeout(30)
class NicSetConnectionPoolTest {

	private static final Inet4Address SERVER = loopback("127.0.0.1");
	private static final Inet4Address NIC_A = loopback("127.0.0.2");
	private static final Inet4Address NIC_B = loopback("127.0.0.3");
	private static final int CONNECT_TIMEOUT_MILLIS = 5_000;

	private final AtomicLong now = new AtomicLong(1_000);
	private final CountingHandler handler = new CountingHandler();
	private final List<EventLoopGroup> groups = new CopyOnWriteArrayList<>();
	private final List<AutoCloseable> resources = new CopyOnWriteArrayList<>();
	private final List<Boolean> reportedOnLoop = new CopyOnWriteArrayList<>();

	@BeforeEach
	void requireEpoll() {
		assumeTrue(Epoll.isAvailable(), "the NIC-set pool binds through epoll");
	}

	@AfterEach
	void closeAll() throws Exception {
		for (final var resource : resources) {
			resource.close();
		}
		for (final var group : groups) {
			group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
		}
	}

	@Test
	void eachNicBindsItsSourceRunsOnItsOwnLoopsAndConnectsToTheFrozenDestination() throws Exception {
		final var a = listener();
		final var b = listener();
		final var groupA = group("lo-a");
		final var groupB = group("lo-b");
		final var paths = List.of(path("s3-a.example:9000", a), path("s3-b.example:9000", b));
		final var pool = open(4, new OpenNic(nic("lo-a", NIC_A, paths), groupA), new OpenNic(nic("lo-b", NIC_B, paths), groupB));

		final var channels = new ArrayList<Channel>();
		for (var i = 0; i < 4; i++) {
			channels.add(acquire(pool).get(5, TimeUnit.SECONDS));
		}

		for (final var channel : channels) {
			final var path = channel.attr(NicPathPool.ATTR_KEY_PATH).get();
			final var local = (InetSocketAddress) channel.localAddress();
			assertEquals(path.pool().address(), local.getAddress(), "bound to the NIC's address");
			assertTrue(contains(path.pool().group(), channel.eventLoop()), "runs on the NIC's own loops");
			assertEquals(path.destination(), channel.remoteAddress(), "connected to the frozen destination");
			assertEquals(path.endpoint(), channel.attr(ATTR_KEY_NODE).get(), "the node is the configured string");
		}
		assertEquals(List.of(true, true, true, true), reportedOnLoop);
		assertEquals(4, handler.created.get());
		assertEquals(List.of(2, 2), pool.state().stream().map(NicSetConnectionPool.NicState::open).toList());
		final var sources = new ArrayList<InetAddress>();
		sources.addAll(a.sources(2));
		sources.addAll(b.sources(2));
		assertEquals(Map.of(NIC_A, 2L, NIC_B, 2L),
						sources.stream().collect(Collectors.groupingBy(source -> source, Collectors.counting())));

		pool.release(channels);
		assertEquals(List.of(2, 2), pool.state().stream().map(NicSetConnectionPool.NicState::idle).toList());
	}

	@Test
	void anAcquisitionAtTheLimitWaitsForAReleaseOrAFreedSlot() throws Exception {
		final var a = listener();
		final var pool = open(1, new OpenNic(nic("lo-a", NIC_A, List.of(path("s3-a.example:9000", a))), group("lo-a")));
		final var path = pool.pools().get(0).paths().get(0);

		final var first = acquire(pool).get(5, TimeUnit.SECONDS);
		final var waiting = acquire(pool);
		assertFalse(waiting.isDone());
		assertEquals(1, pool.state().get(0).waiting());

		pool.release(first);
		assertSame(first, waiting.get(5, TimeUnit.SECONDS), "a release goes to the waiter");

		final var next = acquire(pool);
		assertFalse(next.isDone());
		first.close().syncUninterruptibly();
		final var reopened = next.get(5, TimeUnit.SECONDS);
		assertNotSame(first, reopened, "a closed connection frees its slot for a new one");
		assertEquals(2, path.connectsStarted());
		assertEquals(1, pool.state().get(0).open());
	}

	@Test
	void rotationTakesANicWithCapacityBeforeOneAtItsLimit() throws Exception {
		final var a = listener();
		final var paths = List.of(path("s3-a.example:9000", a));
		final var pool = open(2, new OpenNic(nic("lo-a", NIC_A, paths), group("lo-a")),
						new OpenNic(nic("lo-b", NIC_B, paths), group("lo-b")));

		final var onA = acquire(pool).get(5, TimeUnit.SECONDS);
		final var onB = acquire(pool).get(5, TimeUnit.SECONDS);
		assertEquals(NIC_A, ((InetSocketAddress) onA.localAddress()).getAddress());
		assertEquals(NIC_B, ((InetSocketAddress) onB.localAddress()).getAddress());
		pool.release(onB);

		// The rotation turns to lo-a, which is at its limit of 1; lo-b has an idle connection.
		assertSame(onB, acquire(pool).get(5, TimeUnit.SECONDS));
		assertEquals(0, pool.pendingCount());
	}

	@Test
	void preOpeningSpreadsEachNicsShareOverItsPaths() throws Exception {
		final var a = listener();
		final var b = listener();
		final var paths = List.of(path("s3-a.example:9000", a), path("s3-b.example:9000", b));

		// P >= N: each NIC opens its share, rotating over its Paths from the first.
		assertEquals(List.of(List.of(2L, 1L), List.of(2L, 1L)), preOpened(6, paths));
		// 0 < P < N: one connection per NIC.
		assertEquals(List.of(List.of(1L, 0L), List.of(1L, 0L)), preOpened(1, paths));
		// Unlimited: one connection per Path.
		assertEquals(List.of(List.of(1L, 1L), List.of(1L, 1L)), preOpened(0, paths));
	}

	@Test
	void aFailedConnectBacksThePathOffWhileIdleConnectionsStillServe() throws Exception {
		final var a = listener();
		final var pool = open(0, new OpenNic(nic("lo-a", NIC_A, List.of(path("s3-a.example:9000", a))), group("lo-a")));
		final var path = pool.pools().get(0).paths().get(0);
		final var first = acquire(pool).get(5, TimeUnit.SECONDS);
		final var second = acquire(pool).get(5, TimeUnit.SECONDS);
		pool.release(first);
		a.stopListening();

		assertSame(first, acquire(pool).get(5, TimeUnit.SECONDS), "an idle connection serves");
		final var refused = failure(acquire(pool));
		assertTrue(String.valueOf(refused.getMessage()).toLowerCase(Locale.ROOT).contains("refused"), refused.toString());
		assertTrue(path.inBackoff());
		assertEquals(3, path.connectsStarted());

		final var rejected = acquire(pool);
		assertTrue(rejected.isDone(), "a Path in back-off fails at once");
		assertInstanceOf(PathBackoffException.class, failure(rejected));
		assertEquals(3, path.connectsStarted(), "and starts no connect");

		pool.release(second);
		assertSame(second, acquire(pool).get(5, TimeUnit.SECONDS), "idle connections serve during back-off");

		advanceMillis(99);
		assertInstanceOf(PathBackoffException.class, failure(acquire(pool)));
		advanceMillis(1);
		final var trial = failure(acquire(pool));
		assertFalse(trial instanceof PathBackoffException, "one trial after the interval: " + trial);
		assertEquals(4, path.connectsStarted());
		assertInstanceOf(PathBackoffException.class, failure(acquire(pool)), "the failed trial doubles the interval");
		assertEquals(4, path.connectsStarted());
	}

	@Test
	void aWaiterWhoseConnectFailsHearsTheFailure() throws Exception {
		final var a = listener();
		final var pool = open(1, new OpenNic(nic("lo-a", NIC_A, List.of(path("s3-a.example:9000", a))), group("lo-a")));
		final var first = acquire(pool).get(5, TimeUnit.SECONDS);
		final var waiting = acquire(pool);
		a.stopListening();

		first.close().syncUninterruptibly();

		final var refused = failure(waiting);
		assertTrue(String.valueOf(refused.getMessage()).toLowerCase(Locale.ROOT).contains("refused"), refused.toString());
		waitFor(() -> pool.state().get(0).open() == 0);
		assertEquals(0, pool.pendingCount());
	}

	@Test
	void theProbeNamesTheNicTheEndpointAndTheReason() throws Exception {
		final var live = listener();
		final var refusedPort = closedPort();
		final var hole = blackHole();
		final var pool = open(4, new OpenNic(nic("lo-a", NIC_A, List.of(
						path("s3-a.example:9000", live),
						new NicSetLayout.PathSpec("s3-b.example:9000", new InetSocketAddress(SERVER, refusedPort)),
						new NicSetLayout.PathSpec("s3-c.example:9000", hole.address()))), group("lo-a")));

		final var failures = pool.probe(300, (nic, destination) -> false);

		assertEquals(List.of(
						"NIC lo-a (127.0.0.2) cannot connect to endpoint s3-b.example:9000 (127.0.0.1:" + refusedPort
										+ "): connection refused",
						"NIC lo-a (127.0.0.2) cannot connect to endpoint s3-c.example:9000 (127.0.0.1:"
										+ hole.address().getPort() + "): no answer within 300 ms"),
						failures);
		assertEquals(NIC_A, live.sources(1).get(0), "the probe binds the NIC");
		// Probes count toward neither the limit nor back-off, and are not pool connections.
		final var state = pool.state().get(0);
		assertEquals(0, state.open());
		assertTrue(state.paths().stream().noneMatch(NicSetConnectionPool.PathState::inBackoff));
		assertTrue(state.paths().stream().allMatch(path -> path.connectsStarted() == 0));
		assertEquals(0, handler.created.get());
	}

	@Test
	void anUnreachableEndpointIsExplainedFromTheRoutingTable() throws Exception {
		final var pool = new NicPathPool(0, "lo-a", NIC_A, group("lo-a"), template(), handler, NicBinding.SOURCE_ADDRESS,
						CONNECT_TIMEOUT_MILLIS, 0,
						List.of(new NicSetLayout.PathSpec("s3-a.example:9000", new InetSocketAddress("192.0.2.30", 9000))),
						now::get);
		final var path = pool.paths().get(0);
		final var noRoute = new NoRouteToHostException("No route to host");
		final var unreachable = new ConnectException("connect(..) failed: Network is unreachable");

		assertEquals("no connected subnet and no gateway route through lo-a reaches 192.0.2.30",
						NicSetConnectionPool.reason(path, unreachable, 300, (nic, destination) -> false));
		assertEquals("a route through lo-a covers 192.0.2.30, but no host answered there (no route to host)",
						NicSetConnectionPool.reason(path, noRoute, 300, (nic, destination) -> nic.equals("lo-a")
										&& destination.getHostAddress().equals("192.0.2.30")));
		assertEquals("no answer within 300 ms",
						NicSetConnectionPool.reason(path, new ConnectException("Connection timed out"), 300, (n, d) -> false));
		assertEquals("Permission denied",
						NicSetConnectionPool.reason(path, new ConnectException("Permission denied"), 300, (n, d) -> false));
	}

	@Test
	void closingAdmissionFailsWaitersAndPendingConnectsAndStartsNoConnect() throws Exception {
		final var hole = blackHole();
		final var pool = open(1, new OpenNic(nic("lo-a", NIC_A,
						List.of(new NicSetLayout.PathSpec("s3-a.example:9000", hole.address()))), group("lo-a")));
		final var path = pool.pools().get(0).paths().get(0);
		final var connecting = acquire(pool);
		final var waiting = acquire(pool);
		assertEquals(2, pool.pendingCount());

		pool.closeAdmission();

		assertTrue(connecting.isDone() && waiting.isDone(), "both fail at once, not at the connect timeout");
		assertTrue(failure(connecting).getMessage().contains("the connection pool is closed"));
		assertTrue(failure(waiting).getMessage().contains("the connection pool is closed"));
		assertEquals(0, pool.pendingCount());
		waitFor(() -> pool.state().get(0).open() == 0);
		assertFalse(path.inBackoff(), "a connect cancelled by stopping is not a Path failure");

		final var late = acquire(pool);
		assertTrue(late.isDone());
		assertInstanceOf(ConnectException.class, failure(late));
		assertEquals(1, path.connectsStarted(), "no connect starts after admission closed");
	}

	@Test
	void closeSettlesEverythingAndClosesEveryConnection() throws Exception {
		final var a = listener();
		final var paths = List.of(path("s3-a.example:9000", a));
		final var groupA = group("lo-a");
		final var groupB = group("lo-b");
		final var pool = open(2, new OpenNic(nic("lo-a", NIC_A, paths), groupA), new OpenNic(nic("lo-b", NIC_B, paths), groupB));
		final var leased = acquire(pool).get(5, TimeUnit.SECONDS);
		final var idle = acquire(pool).get(5, TimeUnit.SECONDS);
		pool.release(idle);
		// lo-a is at its limit of 1, so an acquisition there waits.
		final var waiting = acquire(pool.pools().get(0));
		assertFalse(waiting.isDone());

		pool.close();

		assertInstanceOf(ConnectException.class, failure(waiting));
		assertTrue(leased.closeFuture().await(5, TimeUnit.SECONDS), "leased connections close");
		assertTrue(idle.closeFuture().await(5, TimeUnit.SECONDS), "idle connections close");
		assertEquals(0, pool.pendingCount());
		pool.release(leased);
		assertInstanceOf(ConnectException.class, failure(acquire(pool)));

		groupA.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
		groupB.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
		pool.close();
		assertEquals(0, pool.pendingCount(), "close is idempotent, also after the loops terminated");
	}

	@Test
	void anUnopenedOrClosedPoolFailsAtOnceAndLeasingWaitsOnTheCaller() throws Exception {
		final var pool = new NicSetConnectionPool(template(), handler, now::get);
		resources.add(pool);
		assertTrue(failure(acquire(pool)).getMessage().contains("not open"));

		final var a = listener();
		final var nics = List.of(new OpenNic(nic("lo-a", NIC_A, List.of(path("s3-a.example:9000", a))), group("lo-a")));
		pool.open(nics, NicBinding.SOURCE_ADDRESS, CONNECT_TIMEOUT_MILLIS, 2);
		assertThrows(IllegalStateException.class, () -> pool.open(nics, NicBinding.SOURCE_ADDRESS, CONNECT_TIMEOUT_MILLIS, 2));

		final var leased = new ArrayList<Channel>();
		assertEquals(2, pool.lease(leased, 2));
		assertTrue(leased.stream().allMatch(Channel::isActive));
		pool.release(leased);
		assertEquals(2, pool.state().get(0).idle());

		pool.close();
		assertTrue(failure(acquire(pool)).getMessage().contains("closed"));
		assertThrows(ConnectException.class, pool::lease);
	}

	// ---------------------------------------------------------------- fixtures

	private List<List<Long>> preOpened(final int concurrency, final List<NicSetLayout.PathSpec> paths) throws Exception {
		final var pool = open(concurrency, new OpenNic(nic("lo-a", NIC_A, paths), group("lo-a")),
						new OpenNic(nic("lo-b", NIC_B, paths), group("lo-b")));
		pool.preOpen(concurrency, 5_000);
		final var state = pool.state();
		for (final var nic : state) {
			assertEquals(nic.open(), nic.idle(), "pre-opened connections wait idle");
		}
		return state.stream()
						.map(nic -> nic.paths().stream().map(NicSetConnectionPool.PathState::connectsStarted).toList())
						.toList();
	}

	private NicSetConnectionPool open(final int concurrency, final OpenNic... nics) {
		final var pool = new NicSetConnectionPool(template(), handler, now::get);
		pool.open(List.of(nics), NicBinding.SOURCE_ADDRESS, CONNECT_TIMEOUT_MILLIS, concurrency);
		resources.add(pool);
		return pool;
	}

	private CompletableFuture<Channel> acquire(final NicSetConnectionPool pool) {
		return acquire(pool::acquire);
	}

	private CompletableFuture<Channel> acquire(final NicPathPool pool) {
		return acquire(pool::acquire);
	}

	private CompletableFuture<Channel> acquire(final Consumer<NicSetConnectionPool.Acquisition> pool) {
		final var result = new CompletableFuture<Channel>();
		pool.accept((channel, failure) -> {
			if (failure != null) {
				result.completeExceptionally(failure);
			} else {
				reportedOnLoop.add(channel.eventLoop().inEventLoop());
				result.complete(channel);
			}
		});
		return result;
	}

	private static Throwable failure(final CompletableFuture<Channel> acquisition) throws Exception {
		final var thrown = assertThrows(ExecutionException.class, () -> acquisition.get(5, TimeUnit.SECONDS));
		return thrown.getCause();
	}

	private EventLoopGroup group(final String nic) {
		final var group = new EpollEventLoopGroup(2, new DefaultThreadFactory("ioWorker-" + nic, true));
		groups.add(group);
		return group;
	}

	private static Bootstrap template() {
		return new Bootstrap().channel(EpollSocketChannel.class);
	}

	private static NicSetLayout.Nic nic(final String name, final Inet4Address address, final List<NicSetLayout.PathSpec> paths) {
		return new NicSetLayout.Nic(name, address, paths);
	}

	private static NicSetLayout.PathSpec path(final String endpoint, final Listener listener) {
		return new NicSetLayout.PathSpec(endpoint, listener.address());
	}

	private Listener listener() throws Exception {
		final var listener = new Listener();
		resources.add(listener);
		return listener;
	}

	private BlackHole blackHole() throws Exception {
		final var hole = new BlackHole();
		resources.add(hole);
		assumeTrue(hole.saturated, "the loopback listener's accept queue did not fill");
		return hole;
	}

	private static int closedPort() throws Exception {
		try (var server = new ServerSocket(0, 1, SERVER)) {
			return server.getLocalPort();
		}
	}

	private void advanceMillis(final long millis) {
		now.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
	}

	private static void waitFor(final BooleanSupplier condition) throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!condition.getAsBoolean()) {
			assertTrue(System.nanoTime() < deadline, "timed out waiting");
			Thread.sleep(10);
		}
	}

	private static boolean contains(final EventLoopGroup group, final EventExecutor loop) {
		for (final var member : group) {
			if (member == loop) {
				return true;
			}
		}
		return false;
	}

	private static Inet4Address loopback(final String literal) {
		try {
			return (Inet4Address) InetAddress.getByName(literal);
		} catch (final Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private static final class CountingHandler extends AbstractChannelPoolHandler {

		private final AtomicInteger created = new AtomicInteger();

		@Override
		public void channelCreated(final Channel channel) {
			created.incrementAndGet();
		}
	}

	/** Accepts and holds connections, recording each one's source address. */
	private static final class Listener implements AutoCloseable {

		private final ServerSocket server = new ServerSocket(0, 64, SERVER);
		private final List<Socket> accepted = new CopyOnWriteArrayList<>();
		private final Thread acceptor = new Thread(this::accept, "nic-listener");

		Listener() throws Exception {
			acceptor.setDaemon(true);
			acceptor.start();
		}

		InetSocketAddress address() {
			return new InetSocketAddress(SERVER, server.getLocalPort());
		}

		/** The sources of the first {@code count} accepted connections, once accepted. */
		List<InetAddress> sources(final int count) throws InterruptedException {
			waitFor(() -> accepted.size() >= count);
			return accepted.stream().map(Socket::getInetAddress).toList();
		}

		/**
		 * New connects are refused once this returns; accepted connections stay open. The JDK defers
		 * closing a listener while a thread is blocked in accept, so this waits until a connect is
		 * refused.
		 */
		void stopListening() throws Exception {
			server.close();
			acceptor.join(TimeUnit.SECONDS.toMillis(5));
			final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (true) {
				try (var probe = new Socket()) {
					probe.connect(address(), 1_000);
				} catch (final ConnectException refused) {
					return;
				}
				assertTrue(System.nanoTime() < deadline, "the listener still accepts after close");
				Thread.sleep(10);
			}
		}

		private void accept() {
			try {
				while (!server.isClosed()) {
					accepted.add(server.accept());
				}
			} catch (final Exception ignored) {
				// Closed by the test.
			}
		}

		@Override
		public void close() throws Exception {
			server.close();
			for (final var socket : accepted) {
				socket.close();
			}
		}
	}

	/**
	 * A listener that never accepts, its accept queue filled: Linux drops further SYNs, so a connect
	 * gets no answer until its timeout.
	 */
	private static final class BlackHole implements AutoCloseable {

		private final ServerSocket server = new ServerSocket(0, 1, SERVER);
		private final List<Socket> fillers = new ArrayList<>();
		private boolean saturated;

		BlackHole() throws Exception {
			for (var i = 0; i < 16 && !saturated; i++) {
				final var socket = new Socket();
				try {
					socket.connect(server.getLocalSocketAddress(), 200);
					fillers.add(socket);
				} catch (final SocketTimeoutException e) {
					socket.close();
					saturated = true;
				}
			}
		}

		InetSocketAddress address() {
			return new InetSocketAddress(SERVER, server.getLocalPort());
		}

		@Override
		public void close() throws Exception {
			for (final var socket : fillers) {
				socket.close();
			}
			server.close();
		}
	}
}
