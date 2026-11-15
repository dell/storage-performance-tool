package com.dell.spt.base.svc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ServiceUtilTest {

	private static final String HOST_PROPERTY = "java.rmi.server.hostname";
	/** A name that never resolves (RFC 6761), so a bind through it cannot reach any registry. */
	private static final String UNREACHABLE_ADVERTISED_HOST = "spt-advertised-host.invalid";
	private static final Pattern STUB_ENDPOINT = Pattern.compile("endpoint:\\[([^\\]]+)\\]");
	private String originalHostProperty;
	private static final AtomicInteger SERVICE_COUNTER = new AtomicInteger(0);

	@BeforeEach
	void rememberHostname() {
		originalHostProperty = System.getProperty(HOST_PROPERTY);
		System.setProperty(HOST_PROPERTY, "127.0.0.1");
	}

	@AfterEach
	void restoreHostname() throws RemoteException {
		ServiceUtil.shutdown();
		// RMI keeps the last hostname it sampled after the property is cleared, so a test's advertised
		// name would reach later tests' stubs. Export once under the default to leave that one.
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
	void resolveReturnsTypedService() throws Exception {
		final int port = nextAvailablePort();
		final TestService service = newService(port);

		ServiceUtil.ensureRmiRegistryIsAvailableAt(port);
		ServiceUtil.create(service, port);

		final String addrWithPort = "127.0.0.1:" + port;

		final TestServiceContract resolvedFromAddress = ServiceUtil.resolve(addrWithPort, service.name(), TestServiceContract.class);
		assertEquals(service.name(), resolvedFromAddress.name());
		assertEquals("pong", resolvedFromAddress.ping());

		final TestServiceContract resolvedWithSplit = ServiceUtil.resolve("127.0.0.1", port, service.name(), TestServiceContract.class);
		assertEquals(service.name(), resolvedWithSplit.name());
		assertEquals("pong", resolvedWithSplit.ping());

		ServiceUtil.close(service);
	}

	/**
	 * A bridge-mode engine advertises an address the registry treats as non-local. A unit test cannot
	 * produce a non-local origin, so the advertised hostname here is one that never resolves: a bind
	 * through it fails, as a bind through the advertised address fails in a bridge container. The
	 * service must still bind and unbind, and the stub it hands out must keep the advertised name.
	 */
	@Test
	void createAndCloseBindThroughTheLocalRegistryNotTheAdvertisedHost() throws Exception {
		System.setProperty(HOST_PROPERTY, UNREACHABLE_ADVERTISED_HOST);
		final int port = nextAvailablePort();
		final TestService service = newService(port);
		final String advertisedUri = "rmi://" + UNREACHABLE_ADVERTISED_HOST + ":" + port + "/" + service.name();

		ServiceUtil.ensureRmiRegistryIsAvailableAt(port);
		assertEquals(advertisedUri, ServiceUtil.create(service, port));

		final Registry registry = LocateRegistry.getRegistry("127.0.0.1", port);
		final Remote stub = registry.lookup(service.name());
		assertEquals(UNREACHABLE_ADVERTISED_HOST + ":" + port, stubEndpoint(stub));

		assertEquals(advertisedUri, ServiceUtil.close(service));
		assertThrows(NotBoundException.class, () -> registry.lookup(service.name()));
	}

	/** Callers look the service up through its advertised URI and call it on the advertised host. */
	@Test
	void lookupThroughTheAdvertisedUriReturnsAStubOnTheAdvertisedHost() throws Exception {
		System.setProperty(HOST_PROPERTY, "localhost");
		final int port = nextAvailablePort();
		final TestService service = newService(port);

		ServiceUtil.ensureRmiRegistryIsAvailableAt(port);
		final String svcUri = ServiceUtil.create(service, port);
		assertEquals("rmi://localhost:" + port + "/" + service.name(), svcUri);

		final TestServiceContract resolved = ServiceUtil.resolve("localhost", port, service.name(), TestServiceContract.class);
		assertEquals("localhost:" + port, stubEndpoint(resolved));
		assertEquals("pong", resolved.ping());

		assertEquals(svcUri, ServiceUtil.close(service));
		assertThrows(NotBoundException.class,
						() -> ServiceUtil.resolve("localhost", port, service.name(), TestServiceContract.class));
	}

	@Test
	void twoServicesShareARegistryAndRebindAfterARestart() throws Exception {
		final int port = nextAvailablePort();
		final TestService first = newService(port);
		final TestService second = newService(port);

		ServiceUtil.create(first, port);
		ServiceUtil.create(second, port);
		assertEquals("pong", ServiceUtil.resolve("127.0.0.1", port, first.name(), TestServiceContract.class).ping());
		assertEquals("pong", ServiceUtil.resolve("127.0.0.1", port, second.name(), TestServiceContract.class).ping());

		ServiceUtil.close(first);
		assertThrows(NotBoundException.class,
						() -> ServiceUtil.resolve("127.0.0.1", port, first.name(), TestServiceContract.class));
		assertEquals("pong", ServiceUtil.resolve("127.0.0.1", port, second.name(), TestServiceContract.class).ping());

		// A restart forgets the registry this process created; the next create finds it again.
		ServiceUtil.shutdown();
		final TestService restarted = new TestService(first.name(), port);
		ServiceUtil.create(restarted, port);
		assertEquals("pong", ServiceUtil.resolve("127.0.0.1", port, first.name(), TestServiceContract.class).ping());
		ServiceUtil.close(restarted);
	}

	/** A registry this process holds but did not create through ServiceUtil, as after a restart. */
	@Test
	void createBindsIntoARegistryThisProcessCreatedElsewhere() throws Exception {
		final int port = nextAvailablePort();
		final Registry registry = LocateRegistry.createRegistry(port);
		try {
			final TestService service = newService(port);
			final String svcUri = ServiceUtil.create(service, port);
			assertEquals("rmi://127.0.0.1:" + port + "/" + service.name(), svcUri);
			assertEquals("pong", ServiceUtil.resolve("127.0.0.1", port, service.name(), TestServiceContract.class).ping());

			assertEquals(svcUri, ServiceUtil.close(service));
			assertThrows(NotBoundException.class, () -> registry.lookup(service.name()));
		} finally {
			UnicastRemoteObject.unexportObject(registry, true);
		}
	}

	/**
	 * An engine cannot export its services on a registry port another process holds: the export
	 * fails before any bind, as it always has.
	 */
	@Test
	void createFailsOnARegistryPortAnotherProcessHolds() throws Exception {
		final int port = nextAvailablePort();
		final Path java = Path.of(System.getProperty("java.home"), "bin", "java");
		final Process holder = new ProcessBuilder(java.toString(), "-cp", System.getProperty("java.class.path"),
						ExternalRmiRegistry.class.getName(), Integer.toString(port))
						.redirectErrorStream(true)
						.start();
		try {
			final BufferedReader out = new BufferedReader(new InputStreamReader(holder.getInputStream(), StandardCharsets.UTF_8));
			// The JVM may print its own notes first, for example "Picked up JAVA_TOOL_OPTIONS".
			String line;
			do {
				line = out.readLine();
			} while (line != null && !"READY".equals(line));
			assertEquals("READY", line);

			final TestService service = newService(port);
			assertThrows(ExportException.class, () -> ServiceUtil.create(service, port));
			final Registry registry = LocateRegistry.getRegistry("127.0.0.1", port);
			assertEquals(0, registry.list().length);
		} finally {
			holder.getOutputStream().close();
			if (!holder.waitFor(10, TimeUnit.SECONDS)) {
				holder.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
			}
		}
	}

	private static String stubEndpoint(final Remote stub) {
		final Matcher matcher = STUB_ENDPOINT.matcher(stub.toString());
		assertTrue(matcher.find(), () -> "no endpoint in " + stub);
		return matcher.group(1);
	}

	private static TestService newService(final int port) throws RemoteException {
		return new TestService("Svc-" + SERVICE_COUNTER.incrementAndGet(), port);
	}

	private static int nextAvailablePort() throws IOException {
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			return socket.getLocalPort();
		}
	}

	private interface TestServiceContract extends Service {
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
