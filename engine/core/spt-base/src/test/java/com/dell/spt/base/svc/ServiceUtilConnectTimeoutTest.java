package com.dell.spt.base.svc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.rmi.ConnectException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.UnknownHostException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** {@link ServiceUtil#resolveWithConnectTimeout} against real loopback sockets, with a short timeout. */
class ServiceUtilConnectTimeoutTest {

	private static final String HOST_PROPERTY = "java.rmi.server.hostname";
	private static final int TIMEOUT_MILLIS = 500;
	private static final AtomicInteger SERVICE_COUNTER = new AtomicInteger(0);
	private String originalHostProperty;

	@BeforeEach
	void rememberHostname() {
		originalHostProperty = System.getProperty(HOST_PROPERTY);
		System.setProperty(HOST_PROPERTY, "127.0.0.1");
	}

	@AfterEach
	void restoreHostname() throws RemoteException {
		ServiceUtil.shutdown();
		// As in ServiceUtilTest: leave RMI's sampled hostname at the default for later tests.
		System.setProperty(HOST_PROPERTY, "127.0.0.1");
		final Remote sample = new Remote() {};
		UnicastRemoteObject.exportObject(sample, 0);
		UnicastRemoteObject.unexportObject(sample, true);
		if (originalHostProperty == null) {
			System.clearProperty(HOST_PROPERTY);
		} else {
			System.setProperty(HOST_PROPERTY, originalHostProperty);
		}
	}

	@Test
	void aDroppedConnectThrowsConnectExceptionAfterTheTimeout() throws Exception {
		try (DroppedConnectTarget target = new DroppedConnectTarget()) {
			final long startNanos = System.nanoTime();
			final ConnectException e = assertThrows(ConnectException.class,
							() -> resolve(target.host() + ":" + target.port(), "file"));
			final long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;

			assertTrue(elapsedMillis >= TIMEOUT_MILLIS - 50, () -> "gave up after " + elapsedMillis + " ms");
			assertTrue(elapsedMillis < TIMEOUT_MILLIS + 2_000, () -> "gave up after " + elapsedMillis + " ms");
			final java.net.ConnectException cause = assertInstanceOf(java.net.ConnectException.class, e.getCause());
			assertInstanceOf(SocketTimeoutException.class, cause.getCause());
		}
	}

	@Test
	void aRefusedConnectThrowsConnectException() throws Exception {
		final int port = closedPort();
		final ConnectException e = assertThrows(ConnectException.class, () -> resolve("127.0.0.1:" + port, "file"));
		assertInstanceOf(java.net.ConnectException.class, e.getCause());
	}

	@Test
	void aLiveRegistryReturnsTheServiceAndCallsOnItDoNotUseTheFactory() throws Exception {
		final int port = freePort();
		final TestService service = newService(port);
		ServiceUtil.create(service, port);
		final CountingFactory factory = new CountingFactory(new ConnectTimeoutClientSocketFactory(TIMEOUT_MILLIS));

		final TestServiceContract resolved = ServiceUtil.resolve("127.0.0.1:" + port, service.name(), TestServiceContract.class, factory);
		final int socketsForTheLookup = factory.sockets.get();
		assertEquals(service.name(), resolved.name());
		assertEquals("pong", resolved.ping());
		assertEquals("pong", resolved.ping());

		assertEquals(1, socketsForTheLookup);
		assertEquals(1, factory.sockets.get());
	}

	@Test
	void theProductionResolveReturnsTheService() throws Exception {
		final int port = freePort();
		final TestService service = newService(port);
		ServiceUtil.create(service, port);

		final TestServiceContract resolved = ServiceUtil.resolveWithConnectTimeout("127.0.0.1:" + port, service.name(), TestServiceContract.class);
		assertEquals("pong", resolved.ping());
	}

	@Test
	void aBracketedIpv6HostIsResolved() throws Exception {
		assumeTrue(ipv6LoopbackWorks(), "no IPv6 loopback on this host");
		final int port = freePort();
		final TestService service = newService(port);
		ServiceUtil.create(service, port);

		final TestServiceContract resolved = resolve("[::1]:" + port, service.name());
		assertEquals(service.name(), resolved.name());
	}

	@Test
	void anUnboundNameThrowsNotBoundException() throws Exception {
		final int port = freePort();
		ServiceUtil.create(newService(port), port);

		assertThrows(NotBoundException.class, () -> resolve("127.0.0.1:" + port, "no-such-service"));
	}

	@Test
	void anUnknownHostThrowsUnknownHostException() {
		assertThrows(UnknownHostException.class, () -> resolve("spt-node.invalid:1", "file"));
	}

	@Test
	void anAddressWithoutAPortThrowsUriSyntaxException() {
		assertThrows(URISyntaxException.class, () -> resolve("127.0.0.1", "file"));
	}

	@Test
	void factoriesWithTheSameTimeoutAreEqual() {
		assertEquals(new ConnectTimeoutClientSocketFactory(1_000), new ConnectTimeoutClientSocketFactory(1_000));
		assertEquals(
						new ConnectTimeoutClientSocketFactory(1_000).hashCode(),
						new ConnectTimeoutClientSocketFactory(1_000).hashCode());
		assertNotEquals(new ConnectTimeoutClientSocketFactory(1_000), new ConnectTimeoutClientSocketFactory(2_000));
		assertThrows(IllegalArgumentException.class, () -> new ConnectTimeoutClientSocketFactory(0));
	}

	private static TestServiceContract resolve(final String addr, final String name) throws Exception {
		return ServiceUtilTestAccess.resolveWithConnectTimeout(addr, name, TestServiceContract.class, TIMEOUT_MILLIS);
	}

	/** {@code ::1}, built from its bytes so that no host name is looked up. */
	private static InetAddress ipv6Loopback() throws IOException {
		final byte[] addr = new byte[16];
		addr[15] = 1;
		return InetAddress.getByAddress(addr);
	}

	private static boolean ipv6LoopbackWorks() {
		try (ServerSocket socket = new ServerSocket(0, 1, ipv6Loopback())) {
			return socket.getInetAddress() instanceof Inet6Address;
		} catch (final IOException e) {
			return false;
		}
	}

	private static int freePort() throws IOException {
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			return socket.getLocalPort();
		}
	}

	private static int closedPort() throws IOException {
		try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			return socket.getLocalPort();
		}
	}

	private static TestService newService(final int port) throws RemoteException {
		return new TestService("ConnectTimeoutSvc-" + SERVICE_COUNTER.incrementAndGet(), port);
	}

	private static final class CountingFactory implements RMIClientSocketFactory {

		private final RMIClientSocketFactory delegate;
		private final AtomicInteger sockets = new AtomicInteger();

		private CountingFactory(final RMIClientSocketFactory delegate) {
			this.delegate = delegate;
		}

		@Override
		public Socket createSocket(final String host, final int port) throws IOException {
			sockets.incrementAndGet();
			return delegate.createSocket(host, port);
		}
	}

	interface TestServiceContract extends Service {
		String ping() throws RemoteException;
	}

	private static final class TestService extends ServiceBase implements TestServiceContract {

		private final String name;

		private TestService(final String name, final int port) throws RemoteException {
			super(port);
			this.name = name;
		}

		@Override
		public String name() {
			return name;
		}

		@Override
		public String ping() {
			return "pong";
		}
	}
}
