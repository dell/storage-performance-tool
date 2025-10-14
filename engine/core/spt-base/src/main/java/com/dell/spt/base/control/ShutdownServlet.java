package com.dell.spt.base.control;

import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.svc.Service;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

/**
 * POST /shutdown — gracefully shuts down the node: closes services; server will stop afterwards
 * in Main.runNode() once services finish (respecting the linger window).
 */
public final class ShutdownServlet extends HttpServlet {

	private final List<Service> services;

	public ShutdownServlet(final List<Service> services) {
		this.services = services;
	}

	@Override
	protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
		// Accept and trigger shutdown asynchronously to avoid blocking the request thread.
		resp.setStatus(HttpServletResponse.SC_ACCEPTED);
		resp.setContentType("application/json");
		resp.getWriter().write("{\"accepted\":true}");
		new Thread(() -> {
			try {
				for (final Service svc : services) {
					try {
						svc.close();
					} catch (final Exception e) {
						Loggers.ERR.warn("Service close failed: {}", e.toString());
					}
				}
			} catch (final Throwable t) {
				Loggers.ERR.warn("Shutdown task failed: {}", t.toString());
			}
		}, "shutdown-servlet-thread").start();
	}
}
