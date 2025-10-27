package com.dell.spt.base.svc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.ServerSocket;
import java.rmi.RemoteException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ServiceUtilTest {

	private static final String HOST_PROPERTY = "java.rmi.server.hostname";
	private String originalHostProperty;
	private static final AtomicInteger SERVICE_COUNTER = new AtomicInteger(0);

	@BeforeEach
	void rememberHostname() {
		originalHostProperty = System.getProperty(HOST_PROPERTY);
		System.setProperty(HOST_PROPERTY, "127.0.0.1");
	}

	@AfterEach
	void restoreHostname() throws RemoteException {
		if (originalHostProperty == null) {
			System.clearProperty(HOST_PROPERTY);
		} else {
			System.setProperty(HOST_PROPERTY, originalHostProperty);
		}
		ServiceUtil.shutdown();
	}

	@Test
	void resolveReturnsTypedService() throws Exception {
		final int port = nextAvailablePort();
		final TestService service = new TestService("Svc-" + SERVICE_COUNTER.incrementAndGet(), port);

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
