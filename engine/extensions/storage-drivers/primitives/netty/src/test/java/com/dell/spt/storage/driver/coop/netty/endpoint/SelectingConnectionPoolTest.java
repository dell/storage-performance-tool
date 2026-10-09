package com.dell.spt.storage.driver.coop.netty.endpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Real loopback sockets; the listeners accept and hold connections until the test closes them. */
@Timeout(30)
class SelectingConnectionPoolTest {

	private final NioEventLoopGroup group = new NioEventLoopGroup(1);
	private final Bootstrap bootstrap = new Bootstrap().group(group).channel(NioSocketChannel.class);
	private final List<Holder> holders = new CopyOnWriteArrayList<>();
	private final List<AutoCloseable> resources = new CopyOnWriteArrayList<>();

	@AfterEach
	void closeAll() throws Exception {
		for (final var holder : holders) {
			holder.close();
		}
		for (final var resource : resources) {
			resource.close();
		}
		group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
	}

	@Test
	void pooledReleaseReusesTheConnectionToTheSameDestination() throws Exception {
		final var a = holder();
		final var pool = pool(List.of(a.address()), true, 4);

		final var first = pool.lease();
		pool.release(first);
		final var second = pool.lease();

		assertSame(first, second);
		assertEquals(1, a.accepted());
		pool.close();
	}

	@Test
	void reusedConnectionIsReportedOnItsOwnEventLoop() throws Exception {
		final var a = holder();
		final var pool = pool(List.of(a.address()), true, 4);
		final var first = pool.lease();
		pool.release(first);
		final var reported = new java.util.concurrent.CompletableFuture<Boolean>();

		// Requests are prepared in the callback; it must never run on the thread that asked.
		pool.acquire((channel, failure) -> reported.complete(
						failure == null && channel == first && channel.eventLoop().inEventLoop()));

		assertTrue(reported.get(5, TimeUnit.SECONDS));
		pool.close();
	}

	@Test
	void closeCancelsAHandOffQueuedOnABusyEventLoop() throws Exception {
		final var a = holder();
		final var pool = pool(List.of(a.address()), true, 4);
		final var idle = pool.lease();
		pool.release(idle);
		final var loopRunning = new java.util.concurrent.CountDownLatch(1);
		final var resumeLoop = new java.util.concurrent.CountDownLatch(1);
		idle.eventLoop().execute(() -> {
			loopRunning.countDown();
			try {
				resumeLoop.await();
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		assertTrue(loopRunning.await(5, TimeUnit.SECONDS));
		final var outcomes = new CopyOnWriteArrayList<Object>();

		// The reused channel is queued for its paused loop; the acquisition must stay cancellable.
		pool.acquire((channel, failure) -> outcomes.add(failure != null ? failure : channel));
		assertEquals(1, pool.pendingAcquisitionCount());
		pool.close();

		assertEquals(1, outcomes.size());
		assertInstanceOf(ConnectException.class, outcomes.get(0));
		assertEquals(0, pool.pendingAcquisitionCount());
		resumeLoop.countDown();
		assertTrue(idle.closeFuture().await(5, TimeUnit.SECONDS));
		Thread.sleep(50);
		assertEquals(1, outcomes.size(), "the late hand-off must not deliver after close");
	}

	@Test
	void pooledReleaseClosesConnectionsBeyondTheIdleLimit() throws Exception {
		final var a = holder();
		final var b = holder();
		final var pool = pool(List.of(a.address(), b.address()), true, 1);

		final var toA = pool.lease();
		final var toB = pool.lease();
		pool.release(toA);
		pool.release(toB);

		assertEquals(1, pool.idleChannelCount());
		assertTrue(toB.closeFuture().await(5, TimeUnit.SECONDS));
		assertTrue(toA.isActive());
		pool.close();
	}

	@Test
	void perRequestReleaseWaitsForTheServerThenClosesAfterTheGracePeriod() throws Exception {
		final var holding = holder();
		final var pool = pool(List.of(holding.address()), false, 0);
		final var channel = pool.lease();

		final var released = System.nanoTime();
		pool.release(channel);

		assertTrue(channel.isActive(), "the client must not close before the server's grace period");
		assertTrue(channel.closeFuture().await(5, TimeUnit.SECONDS));
		final var elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - released);
		assertTrue(elapsed >= EndpointSelectionConstants.SERVER_CLOSE_GRACE_MILLIS - 50, "closed after " + elapsed);
		pool.close();
	}

	@Test
	void perRequestReleaseCompletesAtOnceWhenTheServerClosesFirst() throws Exception {
		final var closing = holder();
		final var pool = pool(List.of(closing.address()), false, 0);
		final var channel = pool.lease();
		closing.closeAccepted();
		assertTrue(channel.closeFuture().await(5, TimeUnit.SECONDS));

		pool.release(channel);

		assertEquals(0, pool.openChannelCount());
		pool.close();
	}

	@Test
	void closeClosesEveryConnectionAndRejectsNewAcquisitions() throws Exception {
		final var a = holder();
		final var pool = pool(List.of(a.address()), true, 4);
		final var leased = pool.lease();
		final var idle = pool.lease();
		pool.release(idle);

		pool.close();

		assertTrue(leased.closeFuture().await(5, TimeUnit.SECONDS));
		assertTrue(idle.closeFuture().await(5, TimeUnit.SECONDS));
		assertEquals(0, pool.idleChannelCount());
		assertThrows(ConnectException.class, pool::lease);
	}

	@Test
	void refusedDestinationFailsTheLeaseWithoutTryingAnother() throws Exception {
		final var live = holder();
		final var pool = pool(List.of(refusedAddress(), live.address()), true, 4);

		assertThrows(ConnectException.class, pool::lease);
		assertEquals(0, live.accepted());
		assertTrue(pool.lease().isActive());
		pool.close();
	}

	@Test
	void everySelectionHasOneOutcomeAndOnlyEstablishedConnectionsClose() throws Exception {
		final var live = holder();
		final var counters = new EndpointSelectionCounters();
		final var pool = pool(List.of(refusedAddress(), live.address()), true, 4, counters);

		assertThrows(ConnectException.class, pool::lease);
		final var first = pool.lease();
		pool.release(first);
		assertThrows(ConnectException.class, pool::lease);
		assertSame(first, pool.lease());
		pool.close();
		stopIoLoop();

		final var snapshot = counters.snapshot();
		assertEquals(1, snapshot.connectsNew());
		assertEquals(1, snapshot.connectsReused());
		assertEquals(2, snapshot.connectsFailed(), "a refused connect is a target failure");
		assertEquals(0, snapshot.connectsCancelled());
		assertEquals(1, snapshot.closes(), "a refused connect never opened a connection");
		assertReconciled(snapshot);
	}

	@Test
	void poolCloseDuringAConnectCountsItAsCancelled() throws Exception {
		final var holding = holder();
		final var counters = new EndpointSelectionCounters();
		final var pool = pool(List.of(holding.address()), false, 0, counters);
		final var resumeLoop = pauseIoLoop();
		final var outcomes = new CopyOnWriteArrayList<Object>();

		// The connection's registration and connect queue behind the paused loop; close() overtakes them.
		pool.acquire((channel, failure) -> outcomes.add(failure != null ? failure : channel));
		pool.close();
		resumeLoop.countDown();
		stopIoLoop();

		assertEquals(1, outcomes.size());
		assertInstanceOf(ConnectException.class, outcomes.get(0));
		assertEquals(0, holding.accepted());
		final var snapshot = counters.snapshot();
		assertEquals(1, snapshot.connectsCancelled());
		assertEquals(0, snapshot.connectsFailed(), "a connect ended by close() is not a target failure");
		assertEquals(0, snapshot.closes(), "the connection never opened");
		assertReconciled(snapshot);
	}

	@Test
	void ioLoopShutdownDuringAConnectCountsItAsCancelled() throws Exception {
		final var counters = new EndpointSelectionCounters();
		final var pool = pool(List.of(unansweredAddress()), false, 0, counters);
		final var outcome = new CompletableFuture<Throwable>();
		pool.acquire((channel, failure) -> outcome.complete(failure));
		Thread.sleep(100);
		assertFalse(outcome.isDone(), "the connect must still be in flight");

		// A stopping driver shuts its I/O loop down before it closes the pool.
		stopIoLoop();
		pool.close();

		assertInstanceOf(ClosedChannelException.class, outcome.get(5, TimeUnit.SECONDS));
		final var snapshot = counters.snapshot();
		assertEquals(1, snapshot.connectsCancelled());
		assertEquals(0, snapshot.connectsFailed(), "a connect ended by shutdown is not a target failure");
		assertEquals(0, snapshot.closes());
		assertReconciled(snapshot);
	}

	@Test
	void bindingIsRequiredOnceBeforeUse() {
		final var pool = new SelectingConnectionPool(bootstrap, new NoopHandler());
		assertThrows(IllegalStateException.class, () -> pool.acquire((channel, failure) -> {}));

		final var destinations = new RoundRobinDestinations(List.of(new InetSocketAddress(InetAddress.getLoopbackAddress(), 9)));
		pool.bind(destinations, true, 1, 1_000, 1_000, new EndpointSelectionCounters());
		assertThrows(IllegalStateException.class, () -> pool.bind(destinations, true, 1, 1_000, 1_000, new EndpointSelectionCounters()));
		pool.close();
	}

	@Test
	void closeSettlesAnAcquisitionStillWaitingForItsSelectionExactlyOnce() throws Exception {
		final Promise<InetSocketAddress> selection = ImmediateEventExecutor.INSTANCE.newPromise();
		final var counters = new EndpointSelectionCounters();
		final var pool = new SelectingConnectionPool(bootstrap, new NoopHandler());
		pool.bind(() -> selection, false, 0, 2_000, 2_000, counters);
		final var outcomes = new CopyOnWriteArrayList<Throwable>();
		final var threads = new CopyOnWriteArrayList<Thread>();
		pool.acquire((channel, failure) -> {
			outcomes.add(failure);
			threads.add(Thread.currentThread());
		});
		assertEquals(1, pool.pendingAcquisitionCount());

		pool.close();
		// A selection that completes afterwards must not connect or report again.
		selection.setSuccess(new InetSocketAddress(InetAddress.getLoopbackAddress(), 9));

		assertEquals(1, outcomes.size());
		assertInstanceOf(ConnectException.class, outcomes.get(0));
		assertSame(Thread.currentThread(), threads.get(0));
		assertEquals(0, pool.pendingAcquisitionCount());
		assertEquals(0, pool.openChannelCount());
		assertEquals(1, counters.snapshot().connectsCancelled());
		assertReconciled(counters.snapshot());
	}

	@Test
	void selectionCompletingAfterTheIoLoopTerminatedSettlesWithoutLeakingAChannel() throws Exception {
		final var holding = holder();
		final Promise<InetSocketAddress> selection = ImmediateEventExecutor.INSTANCE.newPromise();
		final var counters = new EndpointSelectionCounters();
		final var pool = new SelectingConnectionPool(bootstrap, new NoopHandler());
		pool.bind(() -> selection, false, 0, 2_000, 2_000, counters);
		final var outcomes = new CopyOnWriteArrayList<Throwable>();
		pool.acquire((channel, failure) -> outcomes.add(failure));
		stopIoLoop();

		selection.setSuccess(holding.address());
		pool.close();

		assertEquals(1, outcomes.size());
		assertInstanceOf(ConnectException.class, outcomes.get(0));
		assertEquals(0, pool.pendingAcquisitionCount());
		assertEquals(0, pool.openChannelCount());
		assertEquals(0, holding.accepted());
		final var snapshot = counters.snapshot();
		assertEquals(1, snapshot.connectsCancelled());
		assertEquals(0, snapshot.connectsNew() + snapshot.connectsFailed() + snapshot.closes());
		assertReconciled(snapshot);
	}

	@Test
	void unpooledHelperConnectionWaitsForTheServerToClose() throws Exception {
		final var holding = holder();
		final var pool = pool(List.of(holding.address()), false, 0);
		final var channel = pool.connectUnpooled();

		final var closing = System.nanoTime();
		final var unusedClose = channel.close();

		assertTrue(channel.isActive(), "the helper close must wait for the server first");
		assertTrue(channel.closeFuture().await(5, TimeUnit.SECONDS));
		final var elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - closing);
		assertTrue(elapsed >= EndpointSelectionConstants.SERVER_CLOSE_GRACE_MILLIS - 50, "closed after " + elapsed);
		pool.close();
	}

	@Test
	void failureCloseIsNotDeferred() throws Exception {
		final var holding = holder();
		final var pool = pool(List.of(holding.address()), false, 0);
		final var channel = pool.lease();

		channel.close().syncUninterruptibly();

		assertTrue(!channel.isActive());
		pool.close();
	}

	@Test
	void markedConnectionKeepsWaitingWhenClosedAgain() throws Exception {
		final var holding = holder();
		final var pool = pool(List.of(holding.address()), false, 0);
		final var channel = pool.lease();
		pool.awaitServerClose(channel);

		// A protocol handler closes the completed connection, then the driver closes and releases it.
		final var closing = System.nanoTime();
		final var first = channel.close();
		final var second = channel.close();
		pool.release(channel);

		assertTrue(channel.isActive(), "a repeated close must join the wait, not cut it short");
		assertTrue(second.await(5, TimeUnit.SECONDS));
		assertTrue(first.isSuccess() && second.isSuccess());
		final var elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - closing);
		assertTrue(elapsed >= EndpointSelectionConstants.SERVER_CLOSE_GRACE_MILLIS - 50, "closed after " + elapsed);
		pool.close();
	}

	@Test
	void poolCloseEndsTheServerCloseWaitAtOnce() throws Exception {
		final var holding = holder();
		final var pool = pool(List.of(holding.address()), false, 0);
		final var channel = pool.lease();
		pool.release(channel);
		assertTrue(channel.isActive());

		final var closing = System.nanoTime();
		pool.close();

		assertTrue(channel.closeFuture().await(5, TimeUnit.SECONDS));
		final var elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - closing);
		assertTrue(elapsed < EndpointSelectionConstants.SERVER_CLOSE_GRACE_MILLIS / 2, "closed after " + elapsed);
	}

	@Test
	void markingAPooledConnectionHasNoEffect() throws Exception {
		final var holding = holder();
		final var pool = pool(List.of(holding.address()), true, 4);
		final var channel = pool.lease();

		pool.awaitServerClose(channel);
		channel.close().syncUninterruptibly();

		assertTrue(!channel.isActive());
		pool.close();
	}

	private SelectingConnectionPool pool(final List<InetSocketAddress> destinations, final boolean pooled,
					final int idleLimit) {
		return pool(destinations, pooled, idleLimit, new EndpointSelectionCounters());
	}

	private SelectingConnectionPool pool(final List<InetSocketAddress> destinations, final boolean pooled,
					final int idleLimit, final EndpointSelectionCounters counters) {
		final var pool = new SelectingConnectionPool(bootstrap, new NoopHandler());
		pool.bind(new RoundRobinDestinations(destinations), pooled, idleLimit, 2_000, 2_000, counters);
		return pool;
	}

	/** Every counted selection has exactly one outcome, and every established connection has closed. */
	private static void assertReconciled(final EndpointSelectionCounters.Snapshot snapshot) {
		final var selections = snapshot.selections().values().stream().mapToLong(Long::longValue).sum();
		assertEquals(selections, snapshot.connectsNew() + snapshot.connectsReused() + snapshot.connectsFailed()
						+ snapshot.connectsCancelled(), snapshot.toString());
		assertEquals(snapshot.connectsNew(), snapshot.closes(), snapshot.toString());
	}

	/** Shuts the I/O loop down as a stopping driver does; every pending loop task and listener has run after. */
	private void stopIoLoop() {
		group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
	}

	/** Occupies the single I/O loop until the returned latch opens; work handed to the loop queues behind it. */
	private CountDownLatch pauseIoLoop() throws InterruptedException {
		final var running = new CountDownLatch(1);
		final var resume = new CountDownLatch(1);
		group.next().execute(() -> {
			running.countDown();
			try {
				resume.await();
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		assertTrue(running.await(5, TimeUnit.SECONDS));
		return resume;
	}

	private static InetSocketAddress refusedAddress() throws Exception {
		try (final var socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			return new InetSocketAddress(InetAddress.getLoopbackAddress(), socket.getLocalPort());
		}
	}

	/**
	 * A loopback listener whose accept queue is full. Linux then drops further connection requests, so
	 * a connect to it stays in flight. Aborts the test on a platform that answers or refuses instead.
	 */
	private InetSocketAddress unansweredAddress() throws Exception {
		final var listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
		resources.add(listener);
		final var address = new InetSocketAddress(InetAddress.getLoopbackAddress(), listener.getLocalPort());
		for (var i = 0; i < 8; i++) {
			final var queued = new Socket();
			resources.add(queued);
			try {
				queued.connect(address, 250);
			} catch (final SocketTimeoutException full) {
				return address;
			} catch (final ConnectException refused) {
				break;
			}
		}
		return Assumptions.abort("this platform does not leave connects to a full listener in flight");
	}

	private Holder holder() throws Exception {
		final var holder = new Holder();
		holders.add(holder);
		return holder;
	}

	private static final class NoopHandler extends AbstractChannelPoolHandler {
		@Override
		public void channelCreated(final Channel channel) {}
	}

	/** Accepts connections and keeps them open until told otherwise. */
	private static final class Holder implements AutoCloseable {

		private final ServerSocket server = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
		private final List<Socket> accepted = new CopyOnWriteArrayList<>();
		private final Thread acceptor = new Thread(this::accept, "connection-holder");

		Holder() throws Exception {
			acceptor.setDaemon(true);
			acceptor.start();
		}

		InetSocketAddress address() {
			return new InetSocketAddress(InetAddress.getLoopbackAddress(), server.getLocalPort());
		}

		int accepted() throws InterruptedException {
			Thread.sleep(50);
			return accepted.size();
		}

		void closeAccepted() throws Exception {
			final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (accepted.isEmpty() && System.nanoTime() < deadline) {
				Thread.sleep(10);
			}
			for (final var socket : accepted) {
				socket.close();
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
}
