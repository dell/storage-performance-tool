package com.dell.spt.base.svc;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;

import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URISyntaxException;
import java.rmi.RemoteException;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class ServiceBaseTest {

	private static final class TestService extends ServiceBase {

		TestService(final int port) {
			super(port);
		}

		@Override
		protected void doShutdown() {}

		@Override
		public String name() {
			return "test-service";
		}

		void startForTest() {
			doStart();
		}

		void stopForTest() {
			doStop();
		}
	}

	@Test
	void doStartPropagatesFailures() throws RemoteException, URISyntaxException, SocketException, MalformedURLException {
		final TestService svc = new TestService(51234);
		try (MockedStatic<ServiceUtil> util = Mockito.mockStatic(ServiceUtil.class)) {
			util.when(() -> ServiceUtil.create(any(Service.class), anyInt()))
							.thenThrow(new RemoteException("boom"));

			assertThrows(IllegalStateException.class, svc::startForTest);
		}
	}

	@Test
	void doStopPropagatesFailures() throws RemoteException, MalformedURLException {
		final TestService svc = new TestService(51234);
		try (MockedStatic<ServiceUtil> util = Mockito.mockStatic(ServiceUtil.class)) {
			util.when(() -> ServiceUtil.close(any(Service.class)))
							.thenThrow(new RemoteException("stop"));

			assertDoesNotThrow(svc::stopForTest);
		}
	}
}
