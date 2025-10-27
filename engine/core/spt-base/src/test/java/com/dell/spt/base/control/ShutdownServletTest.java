package com.dell.spt.base.control;

import com.dell.spt.base.svc.Service;
import com.dell.spt.base.svc.ServiceBase;
import com.dell.spt.base.svc.ServiceUtil;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import org.mockito.MockedStatic;

class ShutdownServletTest {

	static class LatchingService extends ServiceBase {
		private final CountDownLatch closed = new CountDownLatch(1);

		LatchingService() {
			super(0);
		}

		@Override
		public String name() {
			return "test";
		}

		@Override
		protected void doStop() {
			super.doStop();
			closed.countDown();
		}

		@Override
		public void close() {
			closed.countDown();
		}

		boolean awaitClosed(long t, TimeUnit u) throws InterruptedException {
			return closed.await(t, u);
		}
	}

	@Test
	void postReturns202AndClosesServicesAsync() throws Exception {
		LatchingService svc1 = new LatchingService();
		LatchingService svc2 = new LatchingService();
		ShutdownServlet servlet = new ShutdownServlet(List.of(svc1, svc2));

		httpServletInteractions(servlet, svc1, svc2);
	}

	private void httpServletInteractions(final ShutdownServlet servlet, final LatchingService svc1, final LatchingService svc2)
					throws Exception {
		final HttpServletRequest req = mock(HttpServletRequest.class);
		final HttpServletResponse resp = mock(HttpServletResponse.class);
		final StringWriter sw = new StringWriter();
		when(resp.getWriter()).thenReturn(new PrintWriter(sw));

		try (MockedStatic<ServiceUtil> util = mockStatic(ServiceUtil.class)) {
			util.when(() -> ServiceUtil.close(any(Service.class))).thenReturn("mock://closed");

			servlet.doPost(req, resp);

			verify(resp).setStatus(HttpServletResponse.SC_ACCEPTED);
			assertTrue(sw.toString().contains("accepted"));
			assertTrue(svc1.awaitClosed(2, TimeUnit.SECONDS));
			assertTrue(svc2.awaitClosed(2, TimeUnit.SECONDS));
		}
	}
}
